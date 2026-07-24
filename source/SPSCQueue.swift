import Atomics

/// A bounded, lock-free queue for one producer and one consumer.
///
/// `SPSCQueue` uses a ring buffer with one unused slot to distinguish a full
/// queue from an empty queue. The producer owns the write index and the
/// consumer owns the read index. Acquire/release atomic operations publish
/// initialized elements to the consumer and make consumed slots available to
/// the producer.
///
/// Exactly one thread or task may call producer operations (`push`,
/// `tryPush`, `emplace`, and `tryEmplace`) at a time, and exactly one thread
/// or task may call `pop` at a time. One producer and one consumer may operate
/// concurrently. Using multiple producers or multiple consumers is unsupported.
public final class SPSCQueue<Element>: @unchecked Sendable {
    private let storage: UnsafeMutablePointer<Element>
    private let ringCapacity: Int

    // Each atomic is separately allocated by ManagedAtomic. Keeping producer-
    // and consumer-owned cached indices separate also avoids atomic traffic on
    // the common path.
    private let writeIndex = ManagedAtomic<Int>(0)
    private var cachedReadIndex = 0
    private let readIndex = ManagedAtomic<Int>(0)
    private var cachedWriteIndex = 0

    /// The maximum number of elements the queue can contain.
    public let capacity: Int

    /// Creates a queue that can contain `capacity` elements.
    public init(capacity: Int) {
        precondition(capacity > 0, "capacity must be greater than zero")
        precondition(capacity < Int.max, "capacity is too large")

        self.capacity = capacity
        self.ringCapacity = capacity + 1
        self.storage = .allocate(capacity: capacity + 1)
    }

    deinit {
        // Destruction must happen after the producer and consumer have stopped.
        var index = readIndex.load(ordering: .relaxed)
        let end = writeIndex.load(ordering: .relaxed)
        while index != end {
            storage.advanced(by: index).deinitialize(count: 1)
            index = incremented(index)
        }
        storage.deallocate()
    }

    /// Pushes an element, waiting while the queue is full.
    public func push(_ element: Element) {
        waitForWritableSlot { slot, nextWrite in
            storage.advanced(by: slot).initialize(to: element)
            writeIndex.store(nextWrite, ordering: .releasing)
        }
    }

    /// Tries to push an element without waiting.
    ///
    /// - Returns: `false` when the queue is full; otherwise `true`.
    @discardableResult
    public func tryPush(_ element: Element) -> Bool {
        guard let (slot, nextWrite) = writableSlot() else {
            return false
        }

        storage.advanced(by: slot).initialize(to: element)
        writeIndex.store(nextWrite, ordering: .releasing)
        return true
    }

    /// Constructs and pushes an element, waiting while the queue is full.
    ///
    /// The closure runs only after a writable slot is available. If it throws,
    /// the queue is unchanged.
    public func emplace(_ makeElement: () throws -> Element) rethrows {
        var slotAndNext: (slot: Int, nextWrite: Int)?
        while slotAndNext == nil {
            slotAndNext = writableSlot()
        }

        let element = try makeElement()
        storage.advanced(by: slotAndNext!.slot).initialize(to: element)
        writeIndex.store(slotAndNext!.nextWrite, ordering: .releasing)
    }

    /// Tries to construct and push an element without waiting.
    ///
    /// The closure is not called if the queue is full. If it throws, the queue
    /// is unchanged.
    ///
    /// - Returns: `false` when the queue is full; otherwise `true`.
    @discardableResult
    public func tryEmplace(
        _ makeElement: () throws -> Element
    ) rethrows -> Bool {
        guard let (slot, nextWrite) = writableSlot() else {
            return false
        }

        let element = try makeElement()
        storage.advanced(by: slot).initialize(to: element)
        writeIndex.store(nextWrite, ordering: .releasing)
        return true
    }

    /// Removes and returns the next element, or `nil` when the queue is empty.
    public func pop() -> Element? {
        let read = readIndex.load(ordering: .relaxed)

        if read == cachedWriteIndex {
            cachedWriteIndex = writeIndex.load(ordering: .acquiring)
            if read == cachedWriteIndex {
                return nil
            }
        }

        // move() transfers the value and leaves the slot uninitialized, ready
        // for the producer to initialize again after the release store.
        let element = storage.advanced(by: read).move()
        readIndex.store(incremented(read), ordering: .releasing)
        return element
    }

    /// A moment-in-time element count.
    ///
    /// The value may become stale immediately when producer and consumer are
    /// running concurrently.
    public var count: Int {
        let write = writeIndex.load(ordering: .acquiring)
        let read = readIndex.load(ordering: .acquiring)
        return write >= read ? write - read : write + ringCapacity - read
    }

    /// Whether the queue was empty when its indices were observed.
    public var isEmpty: Bool {
        writeIndex.load(ordering: .acquiring)
            == readIndex.load(ordering: .acquiring)
    }

    private func writableSlot() -> (slot: Int, nextWrite: Int)? {
        let write = writeIndex.load(ordering: .relaxed)
        let nextWrite = incremented(write)

        if nextWrite == cachedReadIndex {
            cachedReadIndex = readIndex.load(ordering: .acquiring)
            if nextWrite == cachedReadIndex {
                return nil
            }
        }

        return (write, nextWrite)
    }

    private func waitForWritableSlot(
        _ body: (_ slot: Int, _ nextWrite: Int) -> Void
    ) {
        while true {
            if let (slot, nextWrite) = writableSlot() {
                body(slot, nextWrite)
                return
            }
        }
    }

    private func incremented(_ index: Int) -> Int {
        let next = index + 1
        return next == ringCapacity ? 0 : next
    }
}
