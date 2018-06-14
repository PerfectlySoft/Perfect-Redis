import XCTest
@testable import PerfectNet
import PerfectThread
@testable import PerfectRedis

class HashTests: XCTestCase {

    override func setUp() {
        NetEvent.initialize()
    }

    override func tearDown() {
        super.tearDown()
        RedisClient.getClient(withIdentifier: clientIdentifier()) {
            c in
            do {
                let client = try c()
                client.flushAll() { _ in }
            } catch {
                print("Failed to clean up after test \(error)")
            }
        }
    }

    func clientIdentifier() -> RedisClientIdentifier {
        return RedisClientIdentifier()
    }

    func testHashSetHashGet() {
        let (key, field, value) = ("mykey", "myfield", "myvalue")
        let expectation = self.expectation(description: "RedisClient")
        RedisClient.getClient(withIdentifier: clientIdentifier()) {
            c in
            do {
                let client = try c()
                client.hashSet(key: key, field: field, value: .string(value)) {
                    response in
                    guard case .integer(let result) = response else {
                        XCTFail("Unexpected response \(response)")
                        expectation.fulfill()
                        return
                    }
                    client.hashGet(key: key, field: field) {
                        response in
                        defer {
                            RedisClient.releaseClient(client)
                            expectation.fulfill()
                        }
                        guard case .bulkString = response else {
                            XCTFail("Unexpected response \(response)")
                            return
                        }
                        let s = response.string
                        XCTAssertEqual(s, value, "Unexpected response \(response)")
                    }
                }
            } catch {
                XCTFail("Could not connect to server \(error)")
                expectation.fulfill()
                return
            }
        }
        self.waitForExpectations(timeout: 60.0) {
            _ in
        }
    }

    /**
    the test sets a field for a hash then:
     - checks for existance of that field in the hash
     - checks if the field can be removed from the hash
     - checks that the field does not exist anymore
     - checks that the field cannot be removed once it does not exist
    */
    func testHashDelAndHashExist() {
        let (key, field, value) = ("mykey", "myfield", "myvalue")
        let expectation = self.expectation(description: "RedisClient")
        RedisClient.getClient(withIdentifier: clientIdentifier()) {
            c in
            do {
                let client = try c()
                client.hashSet(key: key, field: field, value: .string(value)) {
                    _ in
                    client.hashExists(key: key, field: field) {
                        response in
                        let s = response.string
                        XCTAssertEqual(s, "1", "Unexpected response \(response)")
                        client.hashDel(key: key, fields: field) {
                            response in
                            let s = response.string
                            XCTAssertEqual(s, "1", "Unexpected response \(response)")
                            client.hashExists(key: key, field: field) {
                                response in
                                let s = response.string
                                XCTAssertEqual(s, "0", "Unexpected response \(response)")
                                client.hashDel(key: key, fields: field) {
                                    response in
                                    defer {
                                        RedisClient.releaseClient(client)
                                        expectation.fulfill()
                                    }
                                    let s = response.string
                                    XCTAssertEqual(s, "0", "Unexpected response \(response)")
                                }
                            }
                        }
                    }
                }
            } catch {
                XCTFail("Could not connect to server \(error)")
                expectation.fulfill()
                return
            }
        }
        self.waitForExpectations(timeout: 60.0) {
            _ in
        }
    }

    func testHashMultiSetAndHashGetAll() {
        let key = "mykey"
        let hashFieldsValues: [(String, RedisClient.RedisValue)] = [
            ("myfield", .string("myvalue")),
            ("myfield2", .string("myvalue2"))
        ]
        let expectedDict = ["myfield": "myvalue", "myfield2": "myvalue2"]
        let expectation = self.expectation(description: "RedisClient")
        RedisClient.getClient(withIdentifier: clientIdentifier()) {
            c in
            do {
                let client = try c()
                client.hashSet(key: key, fieldsValues: hashFieldsValues) {
                    response in
                    client.hashGetAll(key: key) {
                        response in
                        defer {
                            RedisClient.releaseClient(client)
                            expectation.fulfill()
                        }
                        switch response {
                            case .array(let a):
                                XCTAssertEqual(4, a.count)
                                var resultDict: [String: String] = [:]
                                var ar = a
                                while ar.count > 0 {
                                    let value: String = ar.popLast()!.string!
                                    let key: String = ar.popLast()!.string!
                                    resultDict[key] = value
                                }
                                XCTAssertEqual(expectedDict, resultDict, "Unexpected dictionary returned \(resultDict)")
                            default:
                                XCTFail("Unexpected response \(response)")
                        }
                    }
                }
            } catch {
                XCTFail("Could not connect to server \(error)")
                expectation.fulfill()
                return
            }
        }
        self.waitForExpectations(timeout: 60.0) {
            _ in
        }
    }

    func testHashMultiGet() {
        let key = "mykey"
        let hashFieldsValues: [(String, RedisClient.RedisValue)] = [
            ("myfield", .string("myvalue")),
            ("myfield2", .string("myvalue2")),
            ("myfield3", .string("myvalue3"))
        ]
        let expectedValues = ["myvalue3", "myvalue2"]
        let expectation = self.expectation(description: "RedisClient")
        RedisClient.getClient(withIdentifier: clientIdentifier()) {
            c in
            do {
                let client = try c()
                client.hashSet(key: key, fieldsValues: hashFieldsValues) {
                    response in
                    client.hashGet(key: key, fields: ["myfield3", "myfield2"]) {
                        response in
                        defer {
                            RedisClient.releaseClient(client)
                            expectation.fulfill()
                        }
                        switch response {
                            case .array(let a):
                                let resultArray: [String] = a.map { return $0.string! }
                                XCTAssertEqual(expectedValues, resultArray, "Unexpected array returned \(a)")
                            default:
                                XCTFail("Unexpected response \(response)")
                        }
                    }
                }
            } catch {
                XCTFail("Could not connect to server \(error)")
                expectation.fulfill()
                return
            }
        }
        self.waitForExpectations(timeout: 60.0) {
            _ in
        }
    }

    func testHashKeysValuesLen() {
        let key = "mykey"
        let hashFieldsValues: [(String, RedisClient.RedisValue)] = [
            ("myfield", .string("myvalue")),
            ("myfield2", .string("myvalue2")),
            ("myfield3", .string("myvalue3"))
        ]
        let expectedKeys = hashFieldsValues.map { $0.0 }
        let expectedValues = ["myvalue", "myvalue2", "myvalue3"]
        let expectedLength = hashFieldsValues.count
        let expectation = self.expectation(description: "RedisClient")
        RedisClient.getClient(withIdentifier: clientIdentifier()) {
            c in
            do {
                let client = try c()
                client.hashSet(key: key, fieldsValues: hashFieldsValues) {
                    response in
                    client.hashKeys(key: key) {
                        response in
                        switch response {
                            case .array(let a):
                                let resultArray: [String] = a.map { return $0.string! }
                                XCTAssertEqual(expectedKeys, resultArray, "Unexpected array returned \(a)")
                            default:
                                XCTFail("Unexpected response \(response)")
                        }
						client.hashValues(key: key) {
							response in
							switch response {
								case .array(let a):
									let resultArray: [String] = a.map { return $0.string! }
									XCTAssertEqual(expectedValues, resultArray, "Unexpected array returned \(a)")
								default:
									XCTFail("Unexpected response \(response)")
							}
							client.hashLength(key: key) {
								response in
								switch response {
									case .integer(let len):
										XCTAssertEqual(expectedLength, len, "Unexpected integer returned \(len)")
									default:
										XCTFail("Unexpected response \(response)")
								}
								RedisClient.releaseClient(client)
								expectation.fulfill()
							}
						}
					}
                }
            } catch {
                XCTFail("Could not connect to server \(error)")
                expectation.fulfill()
                return
            }
        }
        self.waitForExpectations(timeout: 60.0) {
            _ in
        }
    }

    func testHashSetNX() {
        let (key, field, value) = ("mykey", "myfield", "myvalue")
        let expectation = self.expectation(description: "RedisClient")
        RedisClient.getClient(withIdentifier: clientIdentifier()) {
            c in
            do {
                let client = try c()
                client.hashSetIfNonExists(key: key, field: field, value: .string(value)) {
                    response in
                    guard case .integer(let result) = response , result == 1 else {
                        XCTFail("Unexpected response \(response)")
                        expectation.fulfill()
                        return
                    }
                    client.hashSetIfNonExists(key: key, field: field, value: .string(value)) {
                        response in
                        defer {
                            RedisClient.releaseClient(client)
                            expectation.fulfill()
                        }

                        guard case .integer(let result) = response , result == 0 else {
                            XCTFail("Unexpected response \(response)")
                            expectation.fulfill()
                            return
                        }
                    }
                }
            } catch {
                XCTFail("Could not connect to server \(error)")
                expectation.fulfill()
                return
            }
        }
        self.waitForExpectations(timeout: 60.0) {
            _ in
        }
    }

    func testHashStrlen() {
        let (key, field, value) = ("mykey", "myfield", "myvalue")
        let expectation = self.expectation(description: "RedisClient")
        RedisClient.getClient(withIdentifier: clientIdentifier()) {
            c in
            do {
                let client = try c()
                client.hashSet(key: key, field: field, value: .string(value)) {
                    response in
                    guard case .integer(let result) = response else {
                        XCTFail("Unexpected response \(response)")
                        expectation.fulfill()
                        return
                    }
                    client.hashStringLength(key: key, field: field) {
                        response in
                        guard case .integer(let result) = response , result == 7 else {
                            XCTFail("Unexpected response \(response)")
                            expectation.fulfill()
                            return
                        }

                        client.hashStringLength(key: key, field: "nonexisting") {
                            response in
                            guard case .integer(let result) = response , result == 0 else {
                                XCTFail("Unexpected response \(response)")
                                expectation.fulfill()
                                return
                            }

                            client.hashStringLength(key: "nonexisting", field: "nonexisting") {
                                response in
                                defer {
                                    RedisClient.releaseClient(client)
                                    expectation.fulfill()
                                }

                                guard case .integer(let result) = response , result == 0 else {
                                    XCTFail("Unexpected response \(response)")
                                    expectation.fulfill()
                                    return
                                }
                            }
                        }
                    }
                }
            } catch {
                XCTFail("Could not connect to server \(error)")
                expectation.fulfill()
                return
            }
        }
        self.waitForExpectations(timeout: 60.0) {
            _ in
        }
    }

    func testHashIncrementBy() {
        let (key, field) = ("mykey", "myfield")
        let expectation = self.expectation(description: "RedisClient")
        RedisClient.getClient(withIdentifier: clientIdentifier()) {
            c in
            do {
                let client = try c()
                client.hashIncrementBy(key: key, field: field, by: 1) {
                    response in
                    guard case .integer(let result) = response , result == 1 else {
                        XCTFail("Unexpected response \(response)")
                        expectation.fulfill()
                        return
                    }
                    client.hashIncrementBy(key: key, field: field, by: -1) {
                        response in
                        guard case .integer(let result) = response , result == 0 else {
                            XCTFail("Unexpected response \(response)")
                            expectation.fulfill()
                            return
                        }

                        client.hashIncrementBy(key: key, field: field, by: 1.5) {
                            response in
                            defer {
                                RedisClient.releaseClient(client)
                                expectation.fulfill()
                            }

                            guard case .bulkString = response else {
                                XCTFail("Unexpected response \(response)")
                                expectation.fulfill()
                                return
                            }

                            let result = Double(response.string!)!
							XCTAssertEqual(1.5, result, accuracy: 0.1, "Unexpected response \(response)")
                        }
                    }
                }
            } catch {
                XCTFail("Could not connect to server \(error)")
                expectation.fulfill()
                return
            }
        }
        self.waitForExpectations(timeout: 60.0) {
            _ in
        }
    }

    func testHashScan() {
        let key = "mykey"
        let hashFieldsValues: [(String, RedisClient.RedisValue)] = [
            ("myfield", .string("myvalue")),
            ("myfield2", .string("myvalue2"))
        ]
        let expectation = self.expectation(description: "RedisClient")
        let expectedDict = ["myfield": "myvalue", "myfield2": "myvalue2"]
        RedisClient.getClient(withIdentifier: clientIdentifier()) {
            c in
            do {
                let client = try c()
                client.hashSet(key: key, fieldsValues: hashFieldsValues) {
                    response in
                    client.hashScan(key: key, cursor: 0) {
                        response in
                        defer {
                            RedisClient.releaseClient(client)
                            expectation.fulfill()
                        }
                        switch response {
                            case .array(let a):
                                XCTAssertEqual("0", a[0].string)
                                var resultDict: [String: String] = [:]
                                switch a[1] {
                                    case .array(var ar):
                                        while ar.count > 0 {
                                            let value: String = ar.popLast()!.string!
                                            let key: String = ar.popLast()!.string!
                                            resultDict[key] = value
                                        }
                                        XCTAssertEqual(expectedDict, resultDict, "Unexpected dictionary returned \(resultDict)")
                                    default:
                                        XCTFail("Was expecting an array of strings as the second element of the response")
                                }
                            default:
                                XCTFail("Unexpected response \(response)")
                        }
                    }
                }
            } catch {
                XCTFail("Could not connect to server \(error)")
                expectation.fulfill()
                return
            }
        }
        self.waitForExpectations(timeout: 60.0) {
            _ in
        }
    }

    static var allTests : [(String, (HashTests) -> () throws -> Void)] {
        return [
            ("testHashSetHashGet", testHashSetHashGet),
            ("testHashDelAndHashExist", testHashDelAndHashExist),
            ("testHashMultiSetAndHashGetAll", testHashMultiSetAndHashGetAll),
            ("testHashMultiGet", testHashMultiGet),
            ("testHashKeysValuesLen", testHashKeysValuesLen),
            ("testHashSetNX", testHashSetNX),
            ("testHashStrlen", testHashStrlen),
            ("testHashIncrementBy", testHashIncrementBy),
            ("testHashScan", testHashScan)
        ]
    }
}
