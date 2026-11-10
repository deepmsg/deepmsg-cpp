/*
 * Dump every labelled counter in a driver's CnC file as `<label>\t<value>`.
 *
 * This is the normaliser behind `run-batch.sh`'s `counters.txt` (see
 * `read.py`'s module note): both archives allocate the reference's counter
 * types under the reference's labels, so a column read from one arm and the
 * same column read from another are the same counter. Unlabelled counters
 * (publication/subscription positions) are skipped: they are addressed by
 * registration id rather than label and are not comparable across arms.
 *
 *   CounterDump.java <cnc-file>
 *
 * Exits non-zero with one line on stderr when the file cannot be mapped, so a
 * batch can leave `counters.txt` absent rather than empty-looking.
 */
import io.aeron.CncFileDescriptor;
import org.agrona.IoUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.nio.charset.StandardCharsets;

import static io.aeron.CncFileDescriptor.createCountersMetaDataBuffer;
import static io.aeron.CncFileDescriptor.createCountersValuesBuffer;
import static io.aeron.CncFileDescriptor.createMetaDataBuffer;

public final class CounterDump
{
    public static void main(final String[] args) throws Exception
    {
        if (args.length != 1)
        {
            System.err.println("usage: CounterDump <cnc-file>");
            System.exit(2);
        }

        final File file = new File(args[0]);
        if (!file.isFile())
        {
            System.err.println("no CnC file at " + file);
            System.exit(1);
        }

        final MappedByteBuffer mapped = IoUtil.mapExistingFile(file, "cnc");
        final UnsafeBuffer meta = createMetaDataBuffer(mapped);
        final CountersReader counters = new CountersReader(
            createCountersMetaDataBuffer(mapped, meta),
            createCountersValuesBuffer(mapped, meta),
            StandardCharsets.US_ASCII);

        counters.forEach((id, typeId, key, label) ->
        {
            if (null != label && !label.isEmpty())
            {
                System.out.println(label + "\t" + counters.getCounterValue(id));
            }
        });
    }
}
