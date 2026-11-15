/*
 * Read or write one of a cluster's control counters in a driver's CnC file.
 *
 *   ClusterToggle.java <cnc-file> <label-prefix>            # read
 *   ClusterToggle.java <cnc-file> <label-prefix> <value>    # write
 *
 * Prints `<counter-id>\t<value>` (the value before the write, when writing) and
 * exits 0 when the prefix names exactly one labelled counter, 1 otherwise, so a
 * script can tell "the node is not there" from "the toggle did not land".
 *
 * An external driver (`deepmsg-media-driver`) has no in-process handle on its
 * counters, so the cluster smoke drives the consensus module's toggles the way
 * the interop tests do -- by writing the counter
 * (`ClusterControl.java:180-205`: 4 = snapshot, 5 = shutdown).
 */
import io.aeron.CncFileDescriptor;
import org.agrona.IoUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static io.aeron.CncFileDescriptor.createCountersMetaDataBuffer;
import static io.aeron.CncFileDescriptor.createCountersValuesBuffer;
import static io.aeron.CncFileDescriptor.createMetaDataBuffer;

public final class ClusterToggle
{
    public static void main(final String[] args) throws Exception
    {
        if (args.length < 2 || args.length > 3)
        {
            System.err.println("usage: ClusterToggle <cnc-file> <label-prefix> [value]");
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
        final UnsafeBuffer values = createCountersValuesBuffer(mapped, meta);
        final CountersReader counters = new CountersReader(
            createCountersMetaDataBuffer(mapped, meta), values, StandardCharsets.US_ASCII);

        final List<Integer> matches = new ArrayList<>();
        counters.forEach((id, typeId, key, label) ->
        {
            if (null != label && label.startsWith(args[1]))
            {
                matches.add(id);
            }
        });

        if (1 != matches.size())
        {
            System.err.println("expected exactly one counter labelled '" + args[1] + "', found " + matches.size());
            System.exit(1);
        }

        final int id = matches.get(0);
        final long before = counters.getCounterValue(id);
        if (3 == args.length)
        {
            new AtomicCounter(values, id).setOrdered(Long.parseLong(args[2]));
        }
        System.out.println(id + "\t" + before);
    }
}
