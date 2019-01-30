# Perfect-Redis [简体中文](README.zh_CN.md)

<p align="center">
    <a href="http://perfect.org/get-involved.html" target="_blank">
        <img src="http://perfect.org/assets/github/perfect_github_2_0_0.jpg" alt="Get Involed with Perfect!" width="854" />
    </a>
</p>

<p align="center">
    <a href="https://github.com/PerfectlySoft/Perfect" target="_blank">
        <img src="http://www.perfect.org/github/Perfect_GH_button_1_Star.jpg" alt="Star Perfect On Github" />
    </a>
    <a href="http://stackoverflow.com/questions/tagged/perfect" target="_blank">
        <img src="http://www.perfect.org/github/perfect_gh_button_2_SO.jpg" alt="Stack Overflow" />
    </a>
    <a href="https://twitter.com/perfectlysoft" target="_blank">
        <img src="http://www.perfect.org/github/Perfect_GH_button_3_twit.jpg" alt="Follow Perfect on Twitter" />
    </a>
    <a href="http://perfect.ly" target="_blank">
        <img src="http://www.perfect.org/github/Perfect_GH_button_4_slack.jpg" alt="Join the Perfect Slack" />
    </a>
</p>

<p align="center">
    <a href="https://developer.apple.com/swift/" target="_blank">
        <img src="https://img.shields.io/badge/Swift-4.0-orange.svg?style=flat" alt="Swift 4.0">
    </a>
    <a href="https://developer.apple.com/swift/" target="_blank">
        <img src="https://img.shields.io/badge/Platforms-OS%20X%20%7C%20Linux%20-lightgray.svg?style=flat" alt="Platforms OS X | Linux">
    </a>
    <a href="http://perfect.org/licensing.html" target="_blank">
        <img src="https://img.shields.io/badge/License-Apache-lightgrey.svg?style=flat" alt="License Apache">
    </a>
    <a href="http://twitter.com/PerfectlySoft" target="_blank">
        <img src="https://img.shields.io/badge/Twitter-@PerfectlySoft-blue.svg?style=flat" alt="PerfectlySoft Twitter">
    </a>
    <a href="http://perfect.ly" target="_blank">
        <img src="http://perfect.ly/badge.svg" alt="Slack Status">
    </a>
</p>

Redis client support for Perfect

## Quick Start

Get a redis client with defaults (localhost, default port):

```swift
let client = RedisClient.getClient(withIdentifier: RedisClientIdentifier())

```

Ping the server:

```swift
let response = client.ping()
guard case .simpleString(let s) = response else {
	return
}
XCTAssert(s == "PONG", "Unexpected response \(response)")
```

Set/get a value:

```swift
let (key, value) = ("mykey", "myvalue")
var response = client.set(key: key, value: .string(value))
guard case .simpleString(let s) = response else {
	...
	return
}
response = client.get(key: key)
guard case .bulkString = response else {
	...
	return
}
let s = response.toString()
XCTAssert(s == value, "Unexpected response \(response)")
```

Pub/sub with two clients using async API:

```swift
RedisClient.getClient(withIdentifier: RedisClientIdentifier()) {
	c in
	do {
		let client1 = try c()
		RedisClient.getClient(withIdentifier: RedisClientIdentifier()) {
			c in
			do {
				let client2 = try c()
				client1.subscribe(channels: ["foo"]) {
					response in
					client2.publish(channel: "foo", message: .string("Hello!")) {
						response in
						client1.readPublished(timeoutSeconds: 5.0) {
							response in
							guard case .array(let array) = response else {
								...
								return
							}
							XCTAssert(array.count == 3, "Invalid array elements")
							XCTAssert(array[0].toString() == "message")
							XCTAssert(array[1].toString() == "foo")
							XCTAssert(array[2].toString() == "Hello!")
						}
					}
				}
			} catch {
				...
			}
		}
	} catch {
		...
	}
}
```

## Building

Add this project as a dependency in your Package.swift file.

```
.package(url: "https://github.com/PerfectlySoft/Perfect-Redis.git", from: "3.2.3")
```

## Further Information
For more information on the Perfect project, please visit [perfect.org](http://perfect.org).
