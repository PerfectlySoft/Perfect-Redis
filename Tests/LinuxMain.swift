import XCTest
@testable import PerfectRedisTests

XCTMain([
     testCase(PerfectRedisTests.allTests),
     testCase(HashTests.allTests)
])
