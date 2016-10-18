
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public abstract class InputFormatAdapter<K, V, T extends InputFormat<K,V>> implements InputFormat<BytesWritable, Text> {

    protected T _inner;

    public InputFormatAdapter(Class<T> t) throws IllegalAccessException, InstantiationException {
        _inner = t.newInstance();
    }

    public InputSplit[] getSplits(JobConf jobConf, int nbSplits) throws IOException {
        return _inner.getSplits(jobConf, nbSplits);
    }

    public RecordReader<BytesWritable, Text> getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
        return new RecordReaderAdapter(this, _inner.getRecordReader(inputSplit, jobConf, reporter));
    }

    private class RecordReaderAdapter implements RecordReader<BytesWritable, Text> {

        InputFormatAdapter<K, V, T> _parent;
        RecordReader<K, V> _inner;
        K _key;
        V _value;

        RecordReaderAdapter(InputFormatAdapter<K, V, T> parent, RecordReader<K, V> inner) {
            _parent = parent;
            _inner = inner;
            _key = _inner.createKey();
            _value = _inner.createValue();
        }

        public boolean next(BytesWritable bytesWritable, Text text) throws IOException {

            do {

                if (!_inner.next(_key, _value)) {
                    return false;
                }

            }
            while(!_parent.filter(_key, _value));

            _parent.convert(_key, _value, bytesWritable);

            return true;
        }

        public BytesWritable createKey() {
            return new BytesWritable();
        }

        public Text createValue() {
            return new Text();
        }

        public long getPos() throws IOException {
            return _inner.getPos();
        }

        public void close() throws IOException {
            _inner.close();
        }

        public float getProgress() throws IOException {
            return _inner.getProgress();
        }
    }

    protected abstract void convert(K key, V value, BytesWritable bytesWritable);

    protected boolean filter(K key, V value)
    {
        return true;
    }
}

abstract class InputFormatJsonAdapter<K, V, T extends InputFormat<K,V>> extends InputFormatAdapter<K, V, T> {

    public InputFormatJsonAdapter(Class<T> t) throws IllegalAccessException, InstantiationException {
        super(t);
    }

    @Override
    protected void convert(K key, V value, BytesWritable bytesWritable) {
        JSON json = convertToJson(key, value);
        byte[] bytes = json.toJSONString().getBytes();
        bytesWritable.set(bytes, 0, bytes.length);
    }

    abstract protected JSON convertToJson(K key, V value);
}

class TotoClass extends InputFormatJsonAdapter<LongWritable, Text, TextInputFormat>
{
    public TotoClass() throws IllegalAccessException, InstantiationException {
        super((Class<TextInputFormat>)new TextInputFormat().getClass());
    }

    @Override
    protected JSON convertToJson(LongWritable key, Text value) {
        JSONObject o = new JSONObject();
        o.put("toto", value.toString());
        return o;
    }
}
