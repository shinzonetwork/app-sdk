# 01 - App SDK

## Status
Written by Quinn Purdy on September 18, 2025

## Context
Applications building on Shinzo are generally expected to be using an instance of DefraDB on their client devices. With this, data relevant to the application (purchased via the relevant Outpost contract) is synced to the user's device to be later queried by the application's runtime client.

## Decision

We will build an app-sdk, that is essentially just a lightweight defra wrapper, for applications building on Shinzo.

Shinzo-team provided applications in the Shinzo network, such as the Indexer and Host client applications will be built on top of this app-sdk.

## Consequences

Some of the known consequences of this setup are:

1. Increased code re-usability for the Shinzo team. This is why we are doing this now. As I've been working on the Indexer and Host clients lately, I noticed I've been writing a lot of similar setup code and helper functions multiple times. While I (and the rest of the Shinzo team) am still learning the Source stack, the Defra integration code is being changed frequently, and these changes often need to occur in multiple places. This is error-prone and time consuming.
2. There are a lot of different ways to configure defra and a lot of different possible ways to integrate them. By providing an out-of-the-box type integration for Shinzo app developers, the Shinzo team reduces our support and testing burden significantly by limiting integration edge cases. Increased flexibility can be introduced as needed (please reach out if you need something!).
3. Providing an SDK of our own making will make writing our documentation and onboarding app developers easier.
4. We can add additional conveniences to our SDK that would not be relevant to general defra users. For instance, we can add, via configuration or parameter, the means to easily filter out responses that do not meet a given attestation(writer signature) count.
5. The Shinzo stack should be refactored to leverage this new SDK. This can be done gradually as needs arise. For example, when writing a test, you may need a convenient way to query a complex object or you may need the means to quickly spin up multiple defra instances and/or multiple instances of whichever service you are working on.