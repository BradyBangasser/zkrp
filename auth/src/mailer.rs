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
            .await?;

        Ok(())
    }
}
