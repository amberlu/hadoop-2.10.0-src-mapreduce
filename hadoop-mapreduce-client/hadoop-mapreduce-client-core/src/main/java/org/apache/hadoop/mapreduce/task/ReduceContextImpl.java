/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce.task;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.BackupStore;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.Progressable;

// COS518 Added
import java.util.*; 
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.MapWritable; 
import org.apache.hadoop.io.IntWritable; 


/**
 * The context passed to the {@link Reducer}.
 * @param <KEYIN> the class of the input keys
 * @param <VALUEIN> the class of the input values
 * @param <KEYOUT> the class of the output keys
 * @param <VALUEOUT> the class of the output values
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ReduceContextImpl<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
    extends TaskInputOutputContextImpl<KEYIN,VALUEIN,KEYOUT,VALUEOUT> 
    implements ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
  private RawKeyValueIterator input;
  private Counter inputValueCounter;
  private Counter inputKeyCounter;
  private RawComparator<KEYIN> comparator;
  private KEYIN key;                                  // current key
  private VALUEIN value;                              // current value 
  private boolean firstValue = false;                 // first value in key
  private boolean nextKeyIsSame = false;              // more w/ this key
  private boolean hasMore;                            // more in file
  protected Progressable reporter;
  private Deserializer<KEYIN> keyDeserializer;
  private Deserializer<VALUEIN> valueDeserializer; 
  private DataInputBuffer buffer = new DataInputBuffer();
  private BytesWritable currentRawKey = new BytesWritable();
  private ValueIterable iterable = new ValueIterable();
  private boolean isMarked = false;
  private BackupStore<KEYIN,VALUEIN> backupStore;
  private final SerializationFactory serializationFactory;
  private final Class<KEYIN> keyClass;
  private final Class<VALUEIN> valueClass;
  private final Configuration conf;
  private final TaskAttemptID taskid;
  private int currentKeyLength = -1;
  private int currentValueLength = -1;

  // COS518 Added 
  private MapWritable newvalue;                       // current MapWritable value if melbourne shuffle enabled
  private Deserializer<MapWritable> newvalueDeserializer; // deserializer for MapWritable objects 
  private boolean melbourneShuffleEnabled = false;    // boolean indicating whether melbourne shuffle is enabled
  private Deserializer<MapWritable> tmpDeserializer;
  private DataInputBuffer tmpInputBuffer;
  private Deserializer<MapWritable> tmpDeserializer;
  private MapWritable tmpOutputVal;

  private static final Log LOG = LogFactory.getLog(ReduceContextImpl.class.getName()); 

  public ReduceContextImpl(Configuration conf, TaskAttemptID taskid,
                           RawKeyValueIterator input, 
                           Counter inputKeyCounter,
                           Counter inputValueCounter,
                           RecordWriter<KEYOUT,VALUEOUT> output,
                           OutputCommitter committer,
                           StatusReporter reporter,
                           RawComparator<KEYIN> comparator,
                           Class<KEYIN> keyClass,
                           Class<VALUEIN> valueClass, Integer melbourneKey) throws InterruptedException, IOException{ // COS518 Edition: take an extra parameter for melbourne shuffle



    super(conf, taskid, output, committer, reporter);
    this.input = input;

    this.inputKeyCounter = inputKeyCounter;
    this.inputValueCounter = inputValueCounter;
    this.comparator = comparator;
    this.serializationFactory = new SerializationFactory(conf);
    this.keyDeserializer = serializationFactory.getDeserializer(keyClass);
    this.keyDeserializer.open(buffer);

    // MARK: COS518 Edition  
    if (melbourneKey != 0) {
      // if melbourne shuffle enabled, use MapWritable deserializer
      this.melbourneShuffleEnabled = true;
      this.newvalueDeserializer = serializationFactory.getDeserializer(MapWritable.class);
      this.newvalueDeserializer.open(buffer);

      tmpDeserializer = serializationFactory.getDeserializer(MapWritable.class);
    } else {
      // otherwise, use the default for value type 
      this.valueDeserializer = serializationFactory.getDeserializer(valueClass);
      this.valueDeserializer.open(buffer);

    } 
    // MARK: End

    hasMore = input.next();
    this.keyClass = keyClass;
    this.valueClass = valueClass;
    this.conf = conf;
    this.taskid = taskid;
  }



  /** Start processing next unique key. */
  public boolean nextKey() throws IOException,InterruptedException {
    while (hasMore && nextKeyIsSame) {
      nextKeyValue();
    }
    if (hasMore) {
      if (inputKeyCounter != null) {
        inputKeyCounter.increment(1);
      }
      return nextKeyValue();
    } else {
      return false;
    }
  }

  // Mark: COS518 Edition

  // Given an input buffer for the record's value (of Type MapWritable)
  // check whether it corresponds to a dummy record or not
  public boolean isDummyRecord(DataInputBuffer inputVal) throws IOException, InterruptedException {
    tmpInputBuffer = new DataInputBuffer();
    tmpDeserializer.open(tmpInputBuffer);
    tmpInputBuffer.reset(inputVal.getData(), inputVal.getPosition(), inputVal.getLength() - inputVal.getPosition());

    tmpOutputVal = new MapWritable();
    tmpOutputVal = tmpDeserializer.deserialize(tmpOutputVal);
    if (tmpOutputVal.containsKey(new IntWritable(0))) {
      return true;
    }
    return false;
  }

  // Find the next valid record 
  // And update certain global fields: hasMore, nextKeyIsSame
  public void findNextValidRecord() throws IOException, InterruptedException{
    if (melbourneShuffleEnabled) {
      if (hasMore) {
        if (isDummyRecord(input.getValue())) { // if next is a dummy record
          hasMore = input.next(); // move on to the next item in input 
          findNextValidRecord();
        } else { // next is a valid record
          DataInputBuffer inputKey = input.getKey();
          nextKeyIsSame = comparator.compare(currentRawKey.getBytes(), 0, // update the field nextKeyIsSame
                                     currentRawKey.getLength(),
                                     inputKey.getData(),
                                     inputKey.getPosition(),
                                     inputKey.getLength() - inputKey.getPosition()
                                         ) == 0;
          return;
        }
      } else { // no more records to process
        nextKeyIsSame = false;
        return;
      }
    }
    return;
  }
  // Mark: End


  /**
   * Advance to the next key/value pair. 
   */
  // COS518 Modification: advance to the next valid key/value pair
  
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    boolean is_dummy = false;

    if (!hasMore) {
      key = null;
      value = null;
      return false;
    }

    firstValue = !nextKeyIsSame;

    DataInputBuffer nextKey = input.getKey();
    currentRawKey.set(nextKey.getData(), nextKey.getPosition(), 
                      nextKey.getLength() - nextKey.getPosition());
    buffer.reset(currentRawKey.getBytes(), 0, currentRawKey.getLength());
    key = keyDeserializer.deserialize(key);

    DataInputBuffer nextVal = input.getValue();
    buffer.reset(nextVal.getData(), nextVal.getPosition(), nextVal.getLength()
        - nextVal.getPosition());

    // MARK: COS518 Edition 
    if (!melbourneShuffleEnabled) {
      // if melbourne shuffle disabled
      // use the default value deserializer 
      value = valueDeserializer.deserialize(value);
    } else {
      // otherwise, use MapWritable deserializer
      // newvalue has type MapWritable <IntWritable, VALUEIN>
      newvalue = newvalueDeserializer.deserialize(newvalue);
      IntWritable validKey = new IntWritable(1);
      if (newvalue.containsKey(validKey)) {
        // if the record is valid, update value
        value = (VALUEIN) newvalue.get(validKey);
      } else {
        // otherwise set is_dummy flag
        is_dummy = true;
      }
    }
    // MARK: End

    currentKeyLength = nextKey.getLength() - nextKey.getPosition();
    currentValueLength = nextVal.getLength() - nextVal.getPosition();

    if (isMarked) {
      // Note: nextVal here can be a MapWritable Object
      // In next(), reading from backupStore will convert MapWritable to VALUEIN  
      backupStore.write(nextKey, nextVal);
    }

    hasMore = input.next();
    if (hasMore) { 
      nextKey = input.getKey();
      nextKeyIsSame = comparator.compare(currentRawKey.getBytes(), 0, 
                                     currentRawKey.getLength(),
                                     nextKey.getData(),
                                     nextKey.getPosition(),
                                     nextKey.getLength() - nextKey.getPosition()
                                         ) == 0;
    } else {
      nextKeyIsSame = false;
    }


    // MARK: COS518 Edition
    if (is_dummy) { 
      // this is a dummy record
      // recursively call nextKeyValue() until it finds a valid record or hits the end
      // TODO: is the first record is dummy, should i update anything?
      return nextKeyValue();
    }    
    // MARK: End

    inputValueCounter.increment(1); // increment the count for valid values 
    findNextValidRecord(); // COS518 Modification: move to the next valid record if any
    return true;
  }

  public KEYIN getCurrentKey() {
    return key;
  }

  @Override
  public VALUEIN getCurrentValue() {
    return value;
  }
  
  BackupStore<KEYIN,VALUEIN> getBackupStore() {
    return backupStore;
  }
  
  protected class ValueIterator implements ReduceContext.ValueIterator<VALUEIN> {

    private boolean inReset = false;
    private boolean clearMarkFlag = false;

    @Override
    public boolean hasNext() { 
      try {
        if (inReset && backupStore.hasNext()) {
          return true;
        } 
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException("hasNext failed", e);
      }
      return firstValue || nextKeyIsSame;
    }

    @Override
    public VALUEIN next() {
      if (inReset) {
        try {
          if (backupStore.hasNext()) {
            backupStore.next();
            DataInputBuffer next = backupStore.nextValue(); 
            buffer.reset(next.getData(), next.getPosition(), next.getLength()
                - next.getPosition());
            
            // MARK: COS518 Edition 
            // TODO: update this 
            // value = valueDeserializer.deserialize(value);
            if (!melbourneShuffleEnabled) { 
              // if melbourne shuffle disabled
              // use the default value deserializer for VALUEIN class 
              value = valueDeserializer.deserialize(value);
            } else { 
              // otherwise, use MapWritable deserializer
              newvalue = newvalueDeserializer.deserialize(newvalue);
              IntWritable validKey = new IntWritable(1);
              if (newvalue.containsKey(validKey)) { 
                // if the record is valid, update value
                value = (VALUEIN) newvalue.get(validKey);
              } else {
                // otherwise, skip this dummy record and continue
                next();
              }
            }
            // MARK: End

            return value;
          } else {
            inReset = false;
            backupStore.exitResetMode();
            if (clearMarkFlag) {
              clearMarkFlag = false;
              isMarked = false;
            }
          }
        } catch (IOException e) {
          e.printStackTrace();
          throw new RuntimeException("next value iterator failed", e);
        }
      } 

      if (firstValue) {
        firstValue = false;
        return value;
      }
      // if this isn't the first record and the next key is different, they
      // can't advance it here.
      if (!nextKeyIsSame) {
        throw new NoSuchElementException("iterate past last value");
      }
      // otherwise, go to the next key/value pair
      try {
        nextKeyValue();
        return value;
        
      } catch (IOException ie) {
        throw new RuntimeException("next value iterator failed", ie);
      } catch (InterruptedException ie) {
        // this is bad, but we can't modify the exception list of java.util
        throw new RuntimeException("next value iterator interrupted", ie);        
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("remove not implemented");
    }

    @Override
    public void mark() throws IOException {
      if (getBackupStore() == null) {
        backupStore = new BackupStore<KEYIN,VALUEIN>(conf, taskid);
      }
      isMarked = true;
      if (!inReset) {
        backupStore.reinitialize();
        if (currentKeyLength == -1) {
          // The user has not called next() for this iterator yet, so
          // there is no current record to mark and copy to backup store.
          return;
        }
        assert (currentValueLength != -1);
        int requestedSize = currentKeyLength + currentValueLength + 
          WritableUtils.getVIntSize(currentKeyLength) +
          WritableUtils.getVIntSize(currentValueLength);
        DataOutputStream out = backupStore.getOutputStream(requestedSize);
        writeFirstKeyValueBytes(out);
        backupStore.updateCounters(requestedSize);
      } else {
        backupStore.mark();
      }
    }

    @Override
    public void reset() throws IOException {
      // We reached the end of an iteration and user calls a 
      // reset, but a clearMark was called before, just throw
      // an exception
      if (clearMarkFlag) {
        clearMarkFlag = false;
        backupStore.clearMark();
        throw new IOException("Reset called without a previous mark");
      }
      
      if (!isMarked) {
        throw new IOException("Reset called without a previous mark");
      }
      inReset = true;
      backupStore.reset();
    }

    @Override
    public void clearMark() throws IOException {
      if (getBackupStore() == null) {
        return;
      }
      if (inReset) {
        clearMarkFlag = true;
        backupStore.clearMark();
      } else {
        inReset = isMarked = false;
        backupStore.reinitialize();
      }
    }
    
    /**
     * This method is called when the reducer moves from one key to 
     * another.
     * @throws IOException
     */
    public void resetBackupStore() throws IOException {
      if (getBackupStore() == null) {
        return;
      }
      inReset = isMarked = false;
      backupStore.reinitialize();
      currentKeyLength = -1;
    }

    /**
     * This method is called to write the record that was most recently
     * served (before a call to the mark). Since the framework reads one
     * record in advance, to get this record, we serialize the current key
     * and value
     * @param out
     * @throws IOException
     */
    private void writeFirstKeyValueBytes(DataOutputStream out) 
    throws IOException {
      assert (getCurrentKey() != null && getCurrentValue() != null);
      WritableUtils.writeVInt(out, currentKeyLength);
      WritableUtils.writeVInt(out, currentValueLength);
      Serializer<KEYIN> keySerializer = 
        serializationFactory.getSerializer(keyClass);
      keySerializer.open(out);
      keySerializer.serialize(getCurrentKey());

      Serializer<VALUEIN> valueSerializer = 
        serializationFactory.getSerializer(valueClass);
      valueSerializer.open(out);
      valueSerializer.serialize(getCurrentValue());

    }
  }

  protected class ValueIterable implements Iterable<VALUEIN> {
    private ValueIterator iterator = new ValueIterator();
    @Override
    public Iterator<VALUEIN> iterator() {
      return iterator;
    } 
  }
  
  /**
   * Iterate through the values for the current key, reusing the same value 
   * object, which is stored in the context.
   * @return the series of values associated with the current key. All of the 
   * objects returned directly and indirectly from this method are reused.
   */
  public 
  Iterable<VALUEIN> getValues() throws IOException, InterruptedException {
    return iterable;
  }
}
