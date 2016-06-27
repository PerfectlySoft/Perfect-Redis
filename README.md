# Perfect-Redis
Redis client support for Perfect

Get a redis client with defaults:

```swift
RedisClient.getClient(withIdentifier: RedisClientIdentifier()) {
	c in
	do {
		let client = try c()
		...
	} catch {
		...
	}
}
```

Ping the server:

```swift
client.ping {
	response in
	defer {
		RedisClient.releaseClient(client)
	}
	guard case .simpleString(let s) = response else {
		...
		return
	}
	XCTAssert(s == "PONG", "Unexpected response \(response)")
}
```

Set/get a value:

```swift
let (key, value) = ("mykey", "myvalue")
client.set(key: key, value: .string(value)) {
	response in
	guard case .simpleString(let s) = response else {
		...
		return
	}
	client.get(key: key) {
		response in
		defer {
			RedisClient.releaseClient(client)
		}
		guard case .bulkString = response else {
			...
			return
		}
		let s = response.toString()
		XCTAssert(s == value, "Unexpected response \(response)")
	}
}
```

Pub/sub:

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
.Package(url: "https://github.com/PerfectlySoft/Perfect-Redis.git", Version(0,0,0)..<Version(10,0,0))
```

## Repository Layout

We have finished refactoring Perfect to support Swift Package Manager. The Perfect project has been split up into the following repositories:

* [Perfect](https://github.com/PerfectlySoft/Perfect) - This repository contains the core PerfectLib and will continue to be the main landing point for the project.
* [PerfectTemplate](https://github.com/PerfectlySoft/PerfectTemplate) - A simple starter project which compiles with SPM into a stand-alone executable HTTP server. This repository is ideal for starting on your own Perfect based project.
* [PerfectDocs](https://github.com/PerfectlySoft/PerfectDocs) - Contains all API reference related material.
* [PerfectExamples](https://github.com/PerfectlySoft/PerfectExamples) - All the Perfect example projects and documentation.
* [PerfectEverything](https://github.com/PerfectlySoft/PerfectEverything) - This umbrella repository allows one to pull in all the related Perfect modules in one go, including the servers, examples, database connectors and documentation. This is a great place to start for people wishing to get up to speed with Perfect.
* [PerfectServer](https://github.com/PerfectlySoft/PerfectServer) - Contains the PerfectServer variants, including the stand-alone HTTP and FastCGI servers. Those wishing to do a manual deployment should clone and build from this repository.
* [Perfect-Redis](https://github.com/PerfectlySoft/Perfect-Redis) - Redis database connector.
* [Perfect-SQLite](https://github.com/PerfectlySoft/Perfect-SQLite) - SQLite3 database connector.
* [Perfect-PostgreSQL](https://github.com/PerfectlySoft/Perfect-PostgreSQL) - PostgreSQL database connector.
* [Perfect-MySQL](https://github.com/PerfectlySoft/Perfect-MySQL) - MySQL database connector.
* [Perfect-MongoDB](https://github.com/PerfectlySoft/Perfect-MongoDB) - MongoDB database connector.
* [Perfect-FastCGI-Apache2.4](https://github.com/PerfectlySoft/Perfect-FastCGI-Apache2.4) - Apache 2.4 FastCGI module; required for the Perfect FastCGI server variant.

The database connectors are all stand-alone and can be used outside of the Perfect framework and server.

## Further Information
For more information on the Perfect project, please visit [perfect.org](http://perfect.org).
