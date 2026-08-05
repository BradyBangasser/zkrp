# Reliability

## Targets

- **SLO: 99.9%** availability per service. This is the internal target every
  service is built and measured against.
- **SLA: 99%** to customers. The gap between the 99.9% target and the 99%
  commitment is deliberate headroom, so a bad window does not immediately breach
  the customer contract.

## How it is measured

Each service exports metrics to Prometheus, so availability and error rate are
measured from real request outcomes rather than asserted. A typical success-rate
query over the request and error counters:

```
sum(rate(requests_total{status!~"5.."}[30d]))
  / sum(rate(requests_total[30d]))
```

> Fill in: the exact metric names your services expose, the scrape setup, and any
> alerting on the error budget.

## Error budget

A 99.9% SLO allows roughly 43 minutes of downtime per 30-day window. Tracking
burn against that budget is what turns the SLO from a number in a README into
something operable: when the budget is healthy you ship, when it is nearly spent
you slow down and stabilize.
