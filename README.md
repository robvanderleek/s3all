# s3do

<div align="center">

![Logo](docs/logo.png)

</div>

<div align="center">

[![Build Status](https://github.com/robvanderleek/s3do/workflows/CICD/badge.svg)](https://github.com/robvanderleek/s3do/actions)
[![DockerHub image pulls](https://img.shields.io/docker/pulls/robvanderleek/s3do)](https://hub.docker.com/repository/docker/robvanderleek/s3do)
[![Dependabot](https://badgen.net/badge/Dependabot/enabled/green?icon=dependabot)](https://dependabot.com/)

</div>

A collection of S3 tools.

* [Installation](#installation)
* [Usage](#usage)
* [Commands](#commands)
  * [Inventory](#inventory)
  * [Tag](#tag)

# Installation

Pull the image from DockerHub and run the container:

```shell
$ docker run robvanderleek/s3do
``` 

You can also clone this repository, install the dependencies (using virtualenv) 
and run the Python scripts:

```shell
$ git clone https://github.com/robvanderleek/s3do.git
$ cd s3do
$ virtualenv -p python3 venv
$ . venv/bin/activate
$ pip install -r requirements.txt
$ ./s3do.py
```

# Usage

Running the `robvanderleek/s3do` docker container is the easiest way to use
the toolset. You need to supply your AWS credentials to the container, there
are two options for that:

1. Mounting a local `.aws` credentials folder:

```shell
$ docker run -v $HOME/.aws:/root/.aws robvanderleek/s3do inventory archpi.dabase.com 
```

2. Passing credentials through enviornment variables:

```shell
$ docker run -e AWS_ACCESS_KEY_ID='...' -e AWS_SECRET_ACCESS_KEY='...' -e AWS_SESSION_TOKEN='...' robvanderleek/s3do inventory archpi.dabase.com
```

# Commands

## Inventory

An inventory file contains on each line a comma separated pair of `"<bucket name>","<object key>`. For example:

```
"MyBucket","path/to/file.txt"
"MyBycket","path/to/another/file.txt"
```

To create an inventory file for all objects in a bucket run:

```shell
$ docker run -v $HOME/.aws:/root/.aws robvanderleek/s3do inventory archpi.dabase.com 
```

Create an inventory file for all objects in a bucket that match the `images` 
prefix:

```shell
$ docker run -v $HOME/.aws:/root/.aws robvanderleek/s3do inventory archpi.dabase.com images
```

## Tag

Tag all objects in a bucket with the tag `foo=bar`:

```shell
$ docker run -v $HOME/.aws:/root/.aws robvanderleek/s3do tag --tag foo=bar archpi.dabase.com 
```

Tag all objects that match the `images` prefix with the tags `foo=bar` and 
`spam=eggs`:

```shell
$ docker run -v $HOME/.aws:/root/.aws robvanderleek/s3do tag --tag foo=bar --tag spam=eggs inventory archpi.dabase.com images
```
