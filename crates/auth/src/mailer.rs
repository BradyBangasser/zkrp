use aws_sdk_sesv2::error::ProvideErrorMetadata;
use aws_sdk_sesv2::types::{Body, Content, Destination, EmailContent, Message};

#[async_trait::async_trait]
pub trait Mailer: Send + Sync {
    async fn send_link(&self, to: &str, token_b64: &str) -> Result<(), anyhow::Error>;
}

const HTML_TEMPLATE: &str = include_str!("../assets/email/verify.html");
const TEXT_TEMPLATE: &str = include_str!("../assets/email/verify.txt");

pub struct SesMailer {
    client: aws_sdk_sesv2::Client,
    from: String,
    link_base: String,
}

impl SesMailer {
    pub fn new(client: aws_sdk_sesv2::Client, from: String, link_base: String) -> Self {
        Self {
            client,
            from,
            link_base,
        }
    }

    fn render(&self, template: &str, verify_url: &str) -> String {
        template.replace("{{VERIFY_URL}}", verify_url)
    }
}

#[async_trait::async_trait]
impl Mailer for SesMailer {
    async fn send_link(&self, to: &str, token_b64: &str) -> Result<(), anyhow::Error> {
        let verify_url = format!("{}#{}", self.link_base, token_b64);

        let html = self.render(HTML_TEMPLATE, &verify_url);
        let text = self.render(TEXT_TEMPLATE, &verify_url);

        let content = EmailContent::builder()
            .simple(
                Message::builder()
                    .subject(
                        Content::builder()
                            .data("Verify your school email")
                            .charset("UTF-8")
                            .build()?,
                    )
                    .body(
                        Body::builder()
                            .html(Content::builder().data(html).charset("UTF-8").build()?)
                            .text(Content::builder().data(text).charset("UTF-8").build()?)
                            .build(),
                    )
                    .build(),
            )
            .build();

        self.client
            .send_email()
            .from_email_address(&self.from)
            .destination(Destination::builder().to_addresses(to).build())
            .content(content)
            .send()
            .await
            .map_err(|e| match e.code() {
                // SES's message can quote the recipient; its code never does.
                Some(code) => anyhow::anyhow!("SES {code}"),
                None => anyhow::anyhow!("SES: {e}"),
            })?;

        Ok(())
    }
}

/// Development mailer: logs the magic link instead of sending it. The
/// recipient address is never logged.
pub struct LogMailer {
    pub link_base: String,
}

#[async_trait::async_trait]
impl Mailer for LogMailer {
    async fn send_link(&self, _to: &str, token_b64: &str) -> Result<(), anyhow::Error> {
        tracing::warn!("MAILER=log, magic link: {}#{}", self.link_base, token_b64);
        Ok(())
    }
}

/// Stopgap while SES is in its sandbox: send through SES, and when SES refuses
/// (an unverified recipient), log the link like `LogMailer` so the operator
/// can pass it on. Not for steady state: logged links are live credentials.
pub struct FallbackMailer {
    pub ses: SesMailer,
    pub log: LogMailer,
}

#[async_trait::async_trait]
impl Mailer for FallbackMailer {
    async fn send_link(&self, to: &str, token_b64: &str) -> Result<(), anyhow::Error> {
        match self.ses.send_link(to, token_b64).await {
            Ok(()) => Ok(()),
            Err(e) => {
                tracing::warn!("{e}; logging the link instead");
                self.log.send_link(to, token_b64).await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn falls_back_to_the_log_when_ses_refuses() {
        let conf = aws_sdk_sesv2::Config::builder()
            .behavior_version(aws_sdk_sesv2::config::BehaviorVersion::latest())
            .region(aws_sdk_sesv2::config::Region::new("us-east-2"))
            .credentials_provider(aws_sdk_sesv2::config::Credentials::for_tests())
            .endpoint_url("http://127.0.0.1:1")
            .build();
        let ses = SesMailer::new(
            aws_sdk_sesv2::Client::from_conf(conf),
            "noreply@example.edu".into(),
            "fratrat://verify".into(),
        );
        assert!(ses.send_link("a@example.edu", "tok").await.is_err());
        let fallback = FallbackMailer {
            ses,
            log: LogMailer { link_base: "fratrat://verify".into() },
        };
        assert!(fallback.send_link("a@example.edu", "tok").await.is_ok());
    }
}
