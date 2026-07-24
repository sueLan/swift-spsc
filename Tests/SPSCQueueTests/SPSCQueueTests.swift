import Dispatch
import XCTest
@testable import SPSCQueue

final class SPSCQueueTests: XCTestCase {
    func testFIFOAndWraparound() {
        let queue = SPSCQueue<String>(capacity: 3)

        XCTAssertTrue(queue.isEmpty)
        XCTAssertEqual(queue.capacity, 3)
        XCTAssertTrue(queue.tryPush("one"))
        queue.push("two")
        XCTAssertTrue(queue.tryEmplace { "three" })
        XCTAssertFalse(queue.tryPush("full"))
        XCTAssertEqual(queue.count, 3)

        XCTAssertEqual(queue.pop(), "one")
        XCTAssertTrue(queue.tryPush("four"))
        XCTAssertEqual(queue.pop(), "two")
        XCTAssertEqual(queue.pop(), "three")
        XCTAssertEqual(queue.pop(), "four")
        XCTAssertNil(queue.pop())
        XCTAssertTrue(queue.isEmpty)
    }

    func testEmplaceIsLazyWhenFull() {
        let queue = SPSCQueue<Int>(capacity: 1)
        queue.emplace { 7 }

        var closureWasCalled = false
        XCTAssertFalse(queue.tryEmplace {
            closureWasCalled = true
            return 8
        })
        XCTAssertFalse(closureWasCalled)
        XCTAssertEqual(queue.pop(), 7)
    }

    func testSupportsValueAndReferenceTypes() {
        struct Message: Equatable {
            let id: Int
            let text: String
        }

        final class Token {
            let value: Int

            init(_ value: Int) {
                self.value = value
            }
        }

        let values = SPSCQueue<Message>(capacity: 2)
        values.push(Message(id: 1, text: "hello"))
        XCTAssertEqual(values.pop(), Message(id: 1, text: "hello"))

        let references = SPSCQueue<Token>(capacity: 1)
        let token = Token(42)
        references.push(token)
        XCTAssertTrue(references.pop() === token)
    }

    func testQueueDestroysBufferedReferences() {
        final class Token {}

        weak var weakToken: Token?
        do {
            let queue = SPSCQueue<Token>(capacity: 1)
            do {
                let token = Token()
                weakToken = token
                queue.push(token)
            }
            XCTAssertNotNil(weakToken)
        }
        XCTAssertNil(weakToken)
    }

    func testThrowingEmplaceLeavesQueueUnchanged() {
        enum TestError: Error {
            case expected
        }

        let queue = SPSCQueue<Int>(capacity: 1)
        XCTAssertThrowsError(try queue.emplace {
            throw TestError.expected
        })
        XCTAssertTrue(queue.isEmpty)
        XCTAssertTrue(queue.tryPush(9))
        XCTAssertEqual(queue.pop(), 9)
    }

    func testSingleProducerSingleConsumer() {
        let operationCount = 100_000
        let queue = SPSCQueue<Int>(capacity: 1_024)
        let result = ConsumerResult()
        let group = DispatchGroup()
        let workerQueue = DispatchQueue(
            label: "SPSCQueueTests.workers",
            attributes: .concurrent
        )

        group.enter()
        workerQueue.async {
            for value in 0..<operationCount {
                while !queue.tryPush(value) {}
            }
            group.leave()
        }

        group.enter()
        workerQueue.async {
            var expected = 0
            while expected < operationCount {
                guard let value = queue.pop() else {
                    continue
                }
                if value != expected {
                    result.mismatch = (expected, value)
                    break
                }
                expected += 1
            }
            result.consumed = expected
            group.leave()
        }

        XCTAssertEqual(group.wait(timeout: .now() + 10), .success)
        XCTAssertNil(result.mismatch)
        XCTAssertEqual(result.consumed, operationCount)
        XCTAssertTrue(queue.isEmpty)
    }
}

private final class ConsumerResult: @unchecked Sendable {
    var mismatch: (expected: Int, actual: Int)?
    var consumed = 0
}
