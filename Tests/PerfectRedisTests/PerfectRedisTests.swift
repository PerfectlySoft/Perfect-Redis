import XCTest
@testable import PerfectNet
import PerfectThread
@testable import PerfectRedis

// Caveat emptor! These tests will FLUSHALL on the Redis host (127.0.0.1)

class PerfectRedisTests: XCTestCase {
   
    override func setUp() {
        NetEvent.initialize()
    }
    
    func clientIdentifier() -> RedisClientIdentifier {
        return RedisClientIdentifier()
    }
    
    func testGetClient() {
        let expectation = self.expectation(description: "RedisClient")
        RedisClient.getClient(withIdentifier: clientIdentifier()) {
            c in
            
            do {
                let client = try c()
                defer {
                    RedisClient.releaseClient(client)
                    expectation.fulfill()
                }
            } catch {
                XCTAssert(false, "Could not connect to server \(error)")
                expectation.fulfill()
                return
            }
        }
        self.waitForExpectations(timeout: 60.0) {
            _ in
        }
    }
    
    func testPing() {
        let expectation = self.expectation(description: "RedisClient")
        RedisClient.getClient(withIdentifier: clientIdentifier()) {
            c in
            do {
                let client = try c()
                client.ping {
                    response in
                    defer {
                        RedisClient.releaseClient(client)
                        expectation.fulfill()
                    }
                    guard case .simpleString(let s) = response else {
                        XCTAssert(false, "Unexpected response \(response)")
                        return
                    }
                    XCTAssert(s == "PONG", "Unexpected response \(response)")
                }
            } catch {
                XCTAssert(false, "Could not connect to server \(error)")
                expectation.fulfill()
                return
            }
        }
        self.waitForExpectations(timeout: 60.0) {
            _ in
        }
    }
    
    func testFlushAll() {
        let expectation = self.expectation(description: "RedisClient")
        RedisClient.getClient(withIdentifier: clientIdentifier()) {
            c in
            do {
                let client = try c()
                client.flushAll {
                    response in
                    defer {
                        RedisClient.releaseClient(client)
                        expectation.fulfill()
                    }
                    guard response.isSimpleOK else {
                        XCTAssert(false, "Unexpected response \(response)")
                        return
                    }
                }
            } catch {
                XCTAssert(false, "Could not connect to server \(error)")
                expectation.fulfill()
                return
            }
        }
        self.waitForExpectations(timeout: 60.0) {
            _ in
        }
    }
    
    func save() {
        let expectation = self.expectation(description: "RedisClient")
        RedisClient.getClient(withIdentifier: clientIdentifier()) {
            c in
            do {
                let client = try c()
                client.save {
                    response in
                    defer {
                        RedisClient.releaseClient(client)
                        expectation.fulfill()
                    }
                    guard response.isSimpleOK else {
                        XCTAssert(false, "Unexpected response \(response)")
                        return
                    }
                }
            } catch {
                XCTAssert(false, "Could not connect to server \(error)")
                expectation.fulfill()
                return
            }
        }
        self.waitForExpectations(timeout: 60.0) {
            _ in
        }
    }
    
    func testSetGet() {
        let (key, value) = ("mykey", "myvalue")
        let expectation = self.expectation(description: "RedisClient")
        RedisClient.getClient(withIdentifier: clientIdentifier()) {
            c in
            do {
                let client = try c()
                client.set(key: key, value: .string(value)) {
                    response in
                    guard response.isSimpleOK else {
                        XCTAssert(false, "Unexpected response \(response)")
                        expectation.fulfill()
                        return
                    }
                    client.get(key: key) {
                        response in
                        defer {
                            RedisClient.releaseClient(client)
                            expectation.fulfill()
                        }
                        guard case .bulkString = response else {
                            XCTAssert(false, "Unexpected response \(response)")
                            return
                        }
                        let s = response.string
                        XCTAssert(s == value, "Unexpected response \(response)")
                    }
                }
            } catch {
                XCTAssert(false, "Could not connect to server \(error)")
                expectation.fulfill()
                return
            }
        }
        self.waitForExpectations(timeout: 60.0) {
            _ in
        }
    }
    
    func testSetGetXX() {
        let (key, value) = ("mykey", "myvalue")
        let expectation = self.expectation(description: "RedisClient")
        RedisClient.getClient(withIdentifier: clientIdentifier()) {
            c in
            do {
                let client = try c()
                client.flushAll {
                    response in
                    guard response.isSimpleOK else {
                        XCTAssert(false, "Unexpected response \(response)")
                        return
                    }
                    // set the string IF IT EXISTS - which is false
                    client.set(key: key, value: .string(value), ifExists: true) {
                        response in
                        
                        guard case .bulkString(let bytes) = response , nil == bytes else {
                            XCTAssert(false, "Unexpected response \(response)")
                            expectation.fulfill()
                            return
                        }
                        client.get(key: key) {
                            response in
                            defer {
                                RedisClient.releaseClient(client)
                                expectation.fulfill()
                            }
                            guard case .bulkString(let bytes) = response , nil == bytes else {
                                XCTAssert(false, "Unexpected response \(response)")
                                return
                            }
                        }
                    }
                }
            } catch {
                XCTAssert(false, "Could not connect to server \(error)")
                expectation.fulfill()
                return
            }
        }
        self.waitForExpectations(timeout: 60.0) {
            _ in
        }
    }
    
    func testSetGetNX() {
        let (key, value, value2) = ("mykey", "myvalue", "myothervalue")
        let expectation = self.expectation(description: "RedisClient")
        RedisClient.getClient(withIdentifier: clientIdentifier()) {
            c in
            do {
                let client = try c()
                client.flushAll {
                    response in
                    guard response.isSimpleOK else {
                        XCTAssert(false, "Unexpected response \(response)")
                        return
                    }
                    client.set(key: key, value: .string(value)) {
                        response in
                        guard response.isSimpleOK else {
                            XCTAssert(false, "Unexpected response \(response)")
                            expectation.fulfill()
                            return
                        }
                        client.get(key: key) {
                            response in
                            
                            guard case .bulkString = response else {
                                XCTAssert(false, "Unexpected response \(response)")
                                return
                            }
                            let s = response.string
                            XCTAssert(s == value, "Unexpected response \(response)")
                            
                            // set the string IF IT DOES NOT EXIST - which is false
                            client.set(key: key, value: .string(value2), ifNotExists: true) {
                                response in
                                
                                guard case .bulkString(let bytes) = response , nil == bytes else {
                                    XCTAssert(false, "Unexpected response \(response)")
                                    expectation.fulfill()
                                    return
                                }
                                
                                client.get(key: key) {
                                    response in
                                    
                                    defer {
                                        RedisClient.releaseClient(client)
                                        expectation.fulfill()
                                    }
                                    
                                    guard case .bulkString = response else {
                                        XCTAssert(false, "Unexpected response \(response)")
                                        return
                                    }
                                    
                                    let s = response.string
                                    XCTAssert(s != value2, "Unexpected response \(response)")
                                }
                            }
                        }
                    }
                }
            } catch {
                XCTAssert(false, "Could not connect to server \(error)")
                expectation.fulfill()
                return
            }
        }
        self.waitForExpectations(timeout: 60.0) {
            _ in
        }
    }
    
    func testSetGetExp() {
        let (key, value) = ("mykey", "myvalue")
        let expectation = self.expectation(description: "RedisClient")
        RedisClient.getClient(withIdentifier: clientIdentifier()) {
            c in
            do {
                let client = try c()
                client.set(key: key, value: .string(value), expires: 2.0) {
                    response in
                    guard response.isSimpleOK else {
                        XCTAssert(false, "Unexpected response \(response)")
                        expectation.fulfill()
                        return
                    }
                    client.get(key: key) {
                        response in
                        guard case .bulkString = response else {
                            XCTAssert(false, "Unexpected response \(response)")
                            return
                        }
                        let s = response.string
                        XCTAssert(s == value, "Unexpected response \(response)")
                        Threading.sleep(seconds: 3.0)
                        client.get(key: key) {
                            response in
                            defer {
                                RedisClient.releaseClient(client)
                                expectation.fulfill()
                            }
                            guard case .bulkString(let bytes) = response , nil == bytes else {
                                XCTAssert(false, "Unexpected response \(response)")
                                return
                            }
                        }
                    }
                }
            } catch {
                XCTAssert(false, "Could not connect to server \(error)")
                expectation.fulfill()
                return
            }
        }
        self.waitForExpectations(timeout: 60.0) {
            _ in
        }
    }
    
    func testExists() {
        let (key, value, key2, value2) = ("mykey", "myvalue", "mykey2", "myvalue2")
        let expectation = self.expectation(description: "RedisClient")
        RedisClient.getClient(withIdentifier: clientIdentifier()) {
            c in
            do {
                let client = try c()
                client.set(key: key, value: .string(value)) {
                    response in
                    guard case .simpleString(let s) = response else {
                        XCTAssert(false, "Unexpected response \(response)")
                        expectation.fulfill()
                        return
                    }
                    
                    client.set(key: key2, value: .string(value2)) {
                        response in
                        guard response.isSimpleOK else {
                            XCTAssert(false, "Unexpected response \(response)")
                            expectation.fulfill()
                            return
                        }
                        client.exists(keys: key, key2, "notthere") {
                            response in
                            defer {
                                RedisClient.releaseClient(client)
                                expectation.fulfill()
                            }
                            guard case .integer(let i) = response , i == 2 else {
                                XCTAssert(false, "Unexpected response \(response)")
                                return
                            }
                        }
                    }
                }
            } catch {
                XCTAssert(false, "Could not connect to server \(error)")
                expectation.fulfill()
                return
            }
        }
        self.waitForExpectations(timeout: 60.0) {
            _ in
        }
    }
    
    func testAppend() {
        let (key, value) = ("mykey", "myvalue")
        let expectation = self.expectation(description: "RedisClient")
        RedisClient.getClient(withIdentifier: clientIdentifier()) {
            c in
            do {
                let client = try c()
                client.set(key: key, value: .string(value)) {
                    response in
                    guard response.isSimpleOK else {
                        XCTAssert(false, "Unexpected response \(response)")
                        expectation.fulfill()
                        return
                    }
                    client.get(key: key) {
                        response in
                        guard case .bulkString = response else {
                            XCTAssert(false, "Unexpected response \(response)")
                            return
                        }
                        let s = response.string
                        XCTAssert(s == value, "Unexpected response \(response)")
                        client.append(key: key, value: .string(value)) {
                            response in
                            guard case .integer(let i) = response , i == value.count*2 else {
                                XCTAssert(false, "Unexpected response \(response)")
                                expectation.fulfill()
                                return
                            }
                            client.get(key: key) {
                                response in
                                defer {
                                    RedisClient.releaseClient(client)
                                    expectation.fulfill()
                                }
                                guard case .bulkString = response else {
                                    XCTAssert(false, "Unexpected response \(response)")
                                    return
                                }
                                let s = response.string
                                XCTAssert(s == value + value, "Unexpected response \(response)")
                            }
                        }
                    }
                }
            } catch {
                XCTAssert(false, "Could not connect to server \(error)")
                expectation.fulfill()
                return
            }
        }
        self.waitForExpectations(timeout: 60.0) {
            _ in
        }
    }
    
    func testPubSub() {
        let expectation1 = self.expectation(description: "RedisClient1")
        let expectation2 = self.expectation(description: "RedisClient2")
        RedisClient.getClient(withIdentifier: self.clientIdentifier()) {
            c in
            do {
                let client1 = try c()
                RedisClient.getClient(withIdentifier: self.clientIdentifier()) {
                    c in
                    do {
                        let client2 = try c()
                        client1.subscribe(channels: ["foo"]) {
                            response in
                            client2.publish(channel: "foo", message: .string("Hello!")) {
                                response in
                                client1.readPublished(timeoutSeconds: 5.0) {
                                    response in
                                    defer {
                                        expectation1.fulfill()
                                        expectation2.fulfill()
                                    }
                                    guard case .array(let array) = response else {
                                        XCTAssert(false, "Invalid response from server")
                                        return
                                    }
                                    XCTAssert(array.count == 3, "Invalid array elements")
                                    XCTAssert(array[0].string == "message")
                                    XCTAssert(array[1].string == "foo")
                                    XCTAssert(array[2].string == "Hello!")
                                }
                            }
                        }
                    } catch {
                        XCTAssert(false, "Could not connect to server \(error)")
                        expectation1.fulfill()
                        expectation2.fulfill()
                        return
                    }
                }
            } catch {
                XCTAssert(false, "Could not connect to server \(error)")
                expectation1.fulfill()
                expectation2.fulfill()
                return
            }
        }
        self.waitForExpectations(timeout: 60.0) {
            _ in
        }
    }
    
    func testSendCommandsAsRESP() {
        let (key, value) = ("mykey", "myvalue")
        let expectation = self.expectation(description: "RedisClient")
        RedisClient.getClient(withIdentifier: clientIdentifier()) {
            c in
            do {
                let client = try c()
                client.sendCommandAsRESP(name: "SET", parameters: [key, value]) {
                    response in
                    guard response.isSimpleOK else {
                        XCTAssert(false, "Unexpected response \(response)")
                        expectation.fulfill()
                        return
                    }
                    client.sendCommandAsRESP(name: "GET", parameters: [key]) {
                        response in
                        defer {
                            RedisClient.releaseClient(client)
                            expectation.fulfill()
                        }
                        guard case .bulkString = response else {
                            XCTAssert(false, "Unexpected response \(response)")
                            return
                        }
                        let s = response.string
                        XCTAssert(s == value, "Unexpected response \(response)")
                    }
                }
            } catch {
                XCTAssert(false, "Could not connect to server \(error)")
                expectation.fulfill()
                return
            }
        }
        self.waitForExpectations(timeout: 60.0) {
            _ in
        }
    }
    
    static var allTests : [(String, (PerfectRedisTests) -> () throws -> Void)] {
        return [
            ("testPing", testPing),
            ("testPubSub", testPubSub),
            ("testGetClient", testGetClient),
            ("testFlushAll", testFlushAll),
            ("testAppend", testAppend),
            ("testSetGet", testSetGet),
            ("testExists", testExists),
            ("testSetGetXX", testSetGetXX),
            ("testSetGetNX", testSetGetNX),
            ("testSetGetExp", testSetGetExp),
            ("testSendCommandsAsRESP", testSendCommandsAsRESP)
        ]
    }
}
