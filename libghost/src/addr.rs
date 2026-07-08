//! Multiaddr reachability filtering.
//!
//! Peers bound to `0.0.0.0` advertise every local interface via identify:
//! loopback, LAN, link-local, CGNAT, and (on iOS) Apple's virtual interfaces.
//! Feeding all of that into Kademlia guarantees dial storms that can only ever
//! succeed when two peers happen to share a LAN — and buries the one address
//! that actually works off-LAN: the relay circuit address.

use libp2p::Multiaddr;
use libp2p::multiaddr::Protocol;

/// True if `addr` is worth putting in a routing table / dialing.
///
/// Relayed (`/p2p-circuit`) addresses always pass — reaching NAT'd peers is the
/// entire point of the relay.
pub fn is_dialable(addr: &Multiaddr) -> bool {
    if addr.iter().any(|p| matches!(p, Protocol::P2pCircuit)) {
        return true;
    }

    match addr.iter().next() {
        Some(Protocol::Ip4(ip)) => {
            let o = ip.octets();
            // 100.64.0.0/10 — carrier-grade NAT (cellular).
            let is_cgnat = o[0] == 100 && (o[1] & 0xC0) == 0x40;
            // 192.0.0.0/24 — IETF protocol assignments; iOS uses 192.0.0.x for
            // its virtual (utun/awdl) interfaces.
            let is_ietf = o[0] == 192 && o[1] == 0 && o[2] == 0;

            !ip.is_loopback()
                && !ip.is_private()
                && !ip.is_link_local()
                && !ip.is_unspecified()
                && !ip.is_documentation()
                && !ip.is_broadcast()
                && !is_cgnat
                && !is_ietf
        }
        Some(Protocol::Ip6(ip)) => {
            !ip.is_loopback() && !ip.is_unspecified() && !is_ipv6_unicast_link_local(&ip)
        }
        // /dns4, /dns6, /dnsaddr — resolved later, keep.
        _ => true,
    }
}

/// `Ipv6Addr::is_unicast_link_local` is unstable; fe80::/10 by hand.
fn is_ipv6_unicast_link_local(ip: &std::net::Ipv6Addr) -> bool {
    (ip.segments()[0] & 0xffc0) == 0xfe80
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ma(s: &str) -> Multiaddr {
        s.parse().unwrap()
    }

    #[test]
    fn rejects_unreachable() {
        for s in [
            "/ip4/127.0.0.1/tcp/55678",
            "/ip4/10.1.83.14/udp/62556/quic-v1",
            "/ip4/169.254.84.232/tcp/55678",
            "/ip4/100.110.160.242/tcp/49737", // CGNAT
            "/ip4/192.0.0.6/udp/62051/quic-v1",
            "/ip4/0.0.0.0/tcp/9000",
            "/ip6/::1/tcp/9000",
            "/ip6/fe80::1/tcp/9000",
        ] {
            assert!(!is_dialable(&ma(s)), "{s} should be rejected");
        }
    }

    #[test]
    fn accepts_reachable_and_circuits() {
        for s in [
            "/ip4/116.203.218.60/tcp/9000",
            "/dns4/relay.a.central.eu.infra.zkrp.net/tcp/9000",
            "/ip4/116.203.218.60/tcp/9000/p2p-circuit",
            // A circuit address whose *first* hop is private still passes.
            "/ip4/10.0.0.1/tcp/9000/p2p-circuit",
        ] {
            assert!(is_dialable(&ma(s)), "{s} should be accepted");
        }
    }
}
