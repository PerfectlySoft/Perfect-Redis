import XCTest

#if !canImport(ObjectiveC)
public func allTests() -> [XCTestCaseEntry] {
    return [
        testCase(PerfectRedisTests.allTests),
        testCase(HashTests.allTests),
    ]
}
#endif
