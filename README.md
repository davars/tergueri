# Tergueri

A very incomplete wrapper for Amazon's AWS SDK for Java.  I'm adding
to this accodring to the needs of my own projects.  If you are reading
this, I highly encourage you to fork and extend!

## Usage

In project.clj:
    :dependencies [[tergueri/tergueri "0.0.1-SNAPSHOT"]]

Create aws.properties somewhere on classpath containing:
    aws.key.access = <access key>
    aws.key.secret = <secret key>

The properties file can also be specified by setting the JVM system
property "aws.properties.file".

## Installation

Download [saxon9he.jar](http://github.com/downloads/davars/tergueri/saxon9he.jar) and install it to your local maven repo:
    $ mvn install:install-file -Dfile=saxon9he.jar -DgroupId=net.sf.saxon \
                     -DartifactId=saxon9he -Dversion=9.0.2j -Dpackaging=jar

Then build and install:
    $ lein deps && lein pom && lein install

## License
    Tergueri
    Copyright (c) David Jack. All rights reserved.

    The use and distribution terms for this software are covered by the
    Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
    which can be found in the file epl-v10.html at the root of this distribution.
    By using this software in any fashion, you are agreeing to be bound by
    the terms of this license.

    You must not remove this notice, or any other, from this software.

