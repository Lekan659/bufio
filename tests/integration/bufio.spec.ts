import { BufIO } from "../../src/io";
import { MemoryStorage } from "../../src/storage";
import { Worker } from "../../src/worker";
import { expect } from "chai";
import { faker } from "@faker-js/faker";

type RecordType = { id: number; value: string };

class TestWorker extends Worker<RecordType, any> {
  processedRecords: RecordType[] = [];
  processingCalls = 0;
  shouldFail = false;
  processingDelay = 0;

  async work(records: RecordType[]): Promise<any[]> {
    this.processingCalls++;
    
    if (this.shouldFail) {
      throw new Error("Integration test worker failed");
    }

    if (this.processingDelay > 0) {
      await new Promise(resolve => setTimeout(resolve, this.processingDelay));
    }

    // process each record
    records.forEach(record => {
      this.processedRecords.push({
        ...record,
        value: `processed-${record.value}`
      });
      console.log(`Processed record ${record.id}: ${record.value}`);
    });
    
    return [];
  }
}

function createTestRecord(extra?: Partial<RecordType>): RecordType {
  return {
    id: faker.number.int({ min: 1, max: 1000 }),
    value: faker.string.alphanumeric(8),
    ...extra,
  };
}

describe("Integration Tests", () => {
  let storage: MemoryStorage<RecordType>;
  let worker: TestWorker;
  let bufIO: BufIO<RecordType, any>;

  beforeEach(() => {
    storage = new MemoryStorage<RecordType>();
    worker = new TestWorker();
  });

  afterEach(() => {
    bufIO && bufIO.stop();
  });

  describe("BufIO Integration Test", () => {
    
    it("pushes records through BufIO and processes with worker", () => {
      bufIO = new BufIO({
        worker,
        storage,
        batchSize: 3
      });

      const record1 = createTestRecord({ id: 1, value: "test-1" });
      const record2 = createTestRecord({ id: 2, value: "test-2" });
      const record3 = createTestRecord({ id: 3, value: "test-3" });

      bufIO.push(record1);
      bufIO.push(record2);
      bufIO.push(record3);

      const storedRecords = storage.get();
      expect(storedRecords.length).to.equal(3);
      expect(storedRecords).to.deep.include(record1);
      expect(storedRecords).to.deep.include(record2);
      expect(storedRecords).to.deep.include(record3);
    });

    it("auto flushes and processes records", async () => {
      bufIO = new BufIO({
        worker,
        storage,
        batchSize: 2,
        flushInterval: 100
      });

      const record1 = createTestRecord({ id: 10, value: "auto-1" });
      const record2 = createTestRecord({ id: 20, value: "auto-2" });
      
      bufIO.push(record1);
      bufIO.push(record2);
      bufIO.start();

      // wait for flush
      await new Promise(resolve => setTimeout(resolve, 150));

      expect(worker.processingCalls).to.equal(1);
      expect(worker.processedRecords).to.have.length(2);
      expect(worker.processedRecords[0].value).to.equal("processed-auto-1");
      expect(worker.processedRecords[1].value).to.equal("processed-auto-2");

      // storage should be empty now
      const remainingRecords = storage.get();
      expect(remainingRecords).to.have.length(0);
    });


    it("handles errors from worker", async () => {
      let errorHappened = false;
      let errorMsg = '';
      let failedRecords: RecordType[] = [];

      bufIO = new BufIO({
        worker,
        storage,
        batchSize: 2,
        flushInterval: 50,
        onError: (error, records) => {
          errorHappened = true;
          errorMsg = error.message;
          failedRecords = records;
        }
      });

      worker.shouldFail = true;

      const record1 = createTestRecord({ id: 100 });
      const record2 = createTestRecord({ id: 200 });
      
      bufIO.push(record1);
      bufIO.push(record2);
      bufIO.start();

      await new Promise(resolve => setTimeout(resolve, 100));

      expect(errorHappened).to.be.true;
      expect(errorMsg).to.equal("Integration test worker failed");
      expect(failedRecords.length).to.equal(2);
      expect(failedRecords).to.deep.include(record1);
      expect(failedRecords).to.deep.include(record2);
    });


    it("waits for flush to complete before stopping", async () => {
      worker.processingDelay = 100; // simulate slow worker

      bufIO = new BufIO({
        worker,
        storage,
        batchSize: 2,
        flushInterval: 50
      });

      bufIO.start();

      const record1 = createTestRecord({ id: 1, value: "flush-test-1" });
      const record2 = createTestRecord({ id: 2, value: "flush-test-2" });

      bufIO.push(record1);
      bufIO.push(record2);

      // call stop while flush is still ongoing
      const stopPromise = bufIO.stop();

      // worker should not have finished yet
      expect(worker.processedRecords.length).to.equal(0);

      await stopPromise; // wait for stop to resolve

      // now flush should be done
      expect(worker.processedRecords.length).to.equal(2);
    });

  });

  
});