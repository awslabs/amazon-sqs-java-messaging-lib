Amazon SQS Java Messaging Library
========================================
The **Amazon SQS Java Messaging Library** holds the Java Message Service compatible classes, that are used
for communicating with Amazon Simple Queue Service. This project builds on top of the AWS SDK for Java to use Amazon SQS as the JMS (as defined in Jakarta [3.1 specification](https://jakarta.ee/specifications/messaging/3.1/)) provider for the messaging applications without running any additional software.

* You can download release builds through the [releases section of this](https://github.com/awslabs/amazon-sqs-java-messaging-lib) project.
* For more information on using the amazon-sqs-java-messaging-lib, see our getting started guide to SQS [here](http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/jmsclient.html).

## Getting Started

* **Sign up for AWS** — Before you begin, you need an AWS account. For more information about creating an AWS account and retrieving your AWS credentials, see [AWS Account and Credentials](http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/java-dg-setup.html) in the AWS SDK for Java Developer Guide.
* **Minimum requirements** — To use the sample application, you'll need Java 8 (or later) and [Maven 3](http://maven.apache.org/). For more information about the requirements, see the [Getting Started](http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/jmsclient.html) section of the Amazon SQS Developer Guide.
* **Download** — Download the [latest release](https://github.com/awslabs/amazon-sqs-java-messaging-lib/releases) or pick it up from Maven:
```xml
  <dependency>
    <groupId>com.amazonaws</groupId>
    <artifactId>amazon-sqs-java-messaging-lib</artifactId>
    <version>2.1.4</version>
    <type>jar</type>
  </dependency>
```
* **Further information** - Read the [API documentation](http://aws.amazon.com/documentation/sqs/).

## ObjectMessage deserialization

The body of a JMS `ObjectMessage` is Java serialized data supplied by whoever sent the message. Java
deserialization runs the `readObject` callback of every class named in that data while reconstructing
it, so deserializing a body from an untrusted sender can execute code inside the consumer. Any
principal able to call `sqs:SendMessage` on the queue controls those bytes.

`ObjectMessage` bodies are therefore deserialized through a serialization filter that restricts which
classes may be reconstructed and bounds the shape of the object graph.

### Trusted classes

By default, only classes **directly in** these packages may be deserialized:

`java.lang`, `java.util`, `java.math`, `java.time`

Subpackages are not included. `java.util.HashMap` is trusted; `java.util.concurrent.ConcurrentHashMap`
is not. Dynamic proxy classes are never trusted. Everything else, including your own application
classes and common JDK types from other packages such as `java.sql.Timestamp`, is rejected with a
`MessageFormatException`.

**This is a behaviour change.** If your application sends its own types in an `ObjectMessage`, opt
those packages in with the `com.amazon.sqs.javamessaging.trustedPackages` system property, as a comma
separated list. Each entry is treated both as an exact class name and as a package whose subpackages
are also trusted:

```
-Dcom.amazon.sqs.javamessaging.trustedPackages=com.example.orders,java.util.concurrent
```

Trust only what you actually exchange. Every trusted package widens the set of classes an attacker can
have instantiated on the consumer, so avoid trusting third party libraries, and in particular avoid
trusting libraries known to carry deserialization gadgets.

### Graph limits

Two limits apply regardless of which classes are trusted, because a payload built entirely from
trusted types can still be hostile:

| Limit | Default | Why |
| --- | --- | --- |
| Object graph depth | 24 | Deserializing nested collections costs time exponential in the nesting depth, since each level rehashes the level below it. A payload of a few kilobytes made only of `HashSet` and `String` can otherwise occupy a consumer thread indefinitely. |
| Array length | 262144 | An array length is carried in the stream as an integer, so a payload of a few bytes can otherwise request an allocation large enough to exhaust the heap. No array in a valid message can be longer than this, given the 256 KiB maximum message size. |

Ordinary payloads are far shallower than the depth limit; a map of lists of maps reaches a depth of
about six. If your application genuinely exchanges deeper graphs, raise the limit with
`com.amazon.sqs.javamessaging.maxDeserializationDepth`. Note that raising it restores the cost
described above in proportion, so raise it only as far as your payloads require.

### Disabling filtering

As a temporary migration aid, `com.amazon.sqs.javamessaging.trustAllPackages=true` disables filtering
and the limits entirely, restoring the behaviour of earlier releases. This reintroduces the remote code
execution exposure described above and should only be used where every sender to the queue is trusted.

If your application already sets a JVM wide filter through `jdk.serialFilter`, that filter continues to
apply; this library's filter is combined with it rather than replacing it, so it cannot widen what you
had already chosen to allow.

Consider whether you need `ObjectMessage` at all. A self describing format such as JSON carried in a
`TextMessage` avoids this class of problem entirely.

## Feedback
* Give us feedback [here](https://github.com/awslabs/amazon-sqs-java-messaging-lib/issues).
* If you'd like to contribute a new feature or bug fix, we'd love to see GitHub pull requests from you.



