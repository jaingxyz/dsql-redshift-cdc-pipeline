# Security Policy

## Reporting a vulnerability

Please do **not** open a public issue for security vulnerabilities.

While this is a sample / reference repository (not a service), please use GitHub's [private vulnerability reporting](https://github.com/jaingxyz/dsql-redshift-cdc-pipeline/security/advisories/new) to send anything sensitive. The maintainer is notified privately and a draft advisory is created automatically.

You can expect an initial response within ~7 days. This is a personal project - fix timelines depend on severity and reachability.

## Scope

In scope:

- The Python CDC processor (`app/cdc_processor.py`) and order simulator (`app/order_simulator.py`).
- The CloudFormation template (`infra/cloudformation.yaml`) and bootstrap shell scripts (`infra/scripts/`).
- SQL schemas in `schema/`.
- Dependency vulnerabilities flagged by Dependabot or `pip-audit`.

Out of scope:

- Vulnerabilities in Aurora DSQL, Kinesis, Lambda, or Redshift themselves (report to AWS).
- Misconfiguration of an end user's AWS account when they deploy this stack.
- Cost overruns from running the bootstrap against a real account - see the README for cost callouts.

## Threat model summary

This repository ships infrastructure-as-code and Python that, when deployed, creates a CDC pipeline in the deployer's AWS account. The IAM roles, Lambda code, and SQL paths in this repository are the relevant trust boundaries. The Lambda uses **parameterized SQL via the Redshift Data API**, never string-concatenated SQL - that's the primary defense against SQL injection from CDC payloads.

## Supported versions

Only `main` is supported. There are no maintained release branches.
