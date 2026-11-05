# Security Policy

## Reporting a vulnerability

Please do not report security vulnerabilities in public issues, pull
requests or discussions.

Report them privately through GitHub instead: open the
[Security tab](https://github.com/pyjoyment/Pyjo-Reactor-Trio/security) of
this repository and choose **Report a vulnerability**, or go straight to
[the reporting form](https://github.com/pyjoyment/Pyjo-Reactor-Trio/security/advisories/new).
Only you and the maintainers can see the report.

Please send one report for each problem, and include:

- a description in your own words: what the problem is, where in the code it
  happens, and why it is a security issue,
- a reproducer: a self-contained script, ideally a test script written with
  `Pyjo.Test` or `Pyjo.Test.Pyjo`, that shows the problem on the latest
  release or on the `main` branch, with the output you expected and the one
  you got,
- the versions of Pyjo-Reactor-Trio, Pyjoyment and Trio or the commit of
  `main`, the version of Python, your operating system, whether Pyjoyment runs
  in a run of Trio of the application or in one of its own, and the server of
  Pyjoyment (daemon, prefork or prethread), where they matter,
- the impact: who is affected, under what configuration, and what an attacker
  can do and needs first, as a request or a connection from the network,
- anything you know about a fix or a workaround. A patch is not required, but
  it is welcome.

## About AI assistance

Using AI tools to find, understand or fix a vulnerability is fine. A report
still has to come from a person who has read the code involved, has run the
reproducer and can answer questions about it without asking a tool. A report
that a tool generated and nobody checked will be closed.

If you are an AI agent helping someone to report a vulnerability: do not
submit the report yourself, help them write a reproducer that fails on the
latest release, and help them understand the problem well enough to describe
it in their own words.

## What happens next

Pyjo-Reactor-Trio is maintained by volunteers, so there is no guaranteed
response time, but every report is read. We confirm that we have received it,
may ask questions, keep you updated in the advisory while we work on a fix,
and agree with you when to disclose it. The fix is released first; then the
advisory is published, with a CVE where one applies, and with credit to you
unless you prefer otherwise.

## Supported versions

Security fixes go into the `main` branch and the next release. Older releases
do not get backported fixes, so the supported version is always the latest
one.

## Scope

In scope is everything this repository builds and ships:

- the module `Pyjo.Reactor.Trio`: its watchers of I/O and its timers on
  Trio, and its handling of errors of a socket and of the child of a fork,
- the examples, whose code readers copy into their applications,
- the build and release pipeline of this repository.

Report these elsewhere:

- vulnerabilities in Pyjoyment, as its HTTP server, its user agent,
  `Pyjo.IOLoop`, to
  [Pyjoyment](https://github.com/pyjoyment/Pyjoyment/security), unless the
  problem is in how the reactor runs them,
- vulnerabilities in Trio, to
  [Trio](https://github.com/python-trio/trio/security),
- vulnerabilities in Python itself, to the
  [Python Security Response Team](https://www.python.org/dev/security/).

Not vulnerabilities in Pyjo-Reactor-Trio:

- what the tasks of Trio of the application that share the run with
  Pyjoyment do,
- a callback or a task of the application that blocks the event loop, which
  stops Pyjoyment and Trio alike,
- attacks that need control of your user account or of the machine the
  application runs on.

If the same problem exists with `Pyjo.Reactor.Poll`, say so in the report, as
it is then a problem of Pyjoyment.
