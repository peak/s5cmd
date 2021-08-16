# Contributing to `s5cmd`

A big welcome 👋 and thank you for considering contributing to `s5cmd` open source project.

  1. [About this document](#about-this-document)
  2. [Getting the code](#getting-the-code)
  3. [Testing](#testing)
  4. [Submitting a Pull Request](#submitting-a-pull-request)


## About this document

This document is a guide for developers interested in contributing to `s5cmd`.

## Getting the code

To download the source code of `s5cmd` you will need `git`. There are multiple ways of installing `git` depending on your operating system. This [document](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git) is a good starting point if you don't have `git` installed already.

Before proposing a change you first need to fork the `s5cmd` Github repository. For a detailed overview on forking, please take a look at the [Github documentation on forking](https://docs.github.com/en/get-started/quickstart/fork-a-repo). In short you need to:
 - fork the `peak/s5cmd` repository
 - clone your fork to your local development environment
 - checkout a new git branch for your proposed changes
 - push changes to your fork
 - open a pull request to `peak/s5cmd`


## Testing

### Running the tests

`s5cmd` has both unit tests and integration tests. Unit tests are used to verify the correctness of units (ie functions), integration tests are used to prevent regressions.

While running integration tests `s5cmd` is built and is ran against a fake (in memory) s3 implementation.

We strongly encourage you to write tests for your proposed changes. You can run the tests with the following command:

```
make test
```

### Running static code analysis tools

Here are the list of tools that are used to check the sanity of the code at compile time:
 - [go vet](https://pkg.go.dev/cmd/vet) 
 - [gofmt](https://blog.golang.org/gofmt) go code formatter
 - [staticcheck](https://staticcheck.io/) go linter
 - [unparam](https://github.com/mvdan/unparam) finds unused parameters

`make check` command runs all the checks.

### Performance

While adding your changes and testing your changes, it would be good to remember that `s5cmd`'s goal is to be the fastest s3 client with a rich set of functionality. Here are some important things to keep in mind:

- Avoid making unnecessary s3 api calls.
- If your workload can be made faster by concurrent execution of several tasks use concurrency constructs.


## Submitting a Pull Request

Once you add your changes and all the tests/checks pass, you can submit your pull request to the `peak/s5cmd` repository. Github will trigger automated tests in Github Actions. All tests and checks will be run on different operating systems including `linux`, `macos` and `windows`. 

An `s5cmd` maintainer will review your pull request. They may suggest updates for clarity and style, or request additional unit or integration tests. 

Once all the tests are green and your pull request has been approved, an `s5cmd` maintainer will merge your changes into the `master` branch. And your changes will be released with the next release of `s5cmd`.

That's it. 

🎉 Happy coding! 🎉
