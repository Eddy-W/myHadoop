package tfidf2;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: deason
 * Date: 7/31/13
 * Time: 9:01 AM
 * To change this template use File | Settings | File Templates.
 */
public class CompositeKeyForTFIDF implements WritableComparable<CompositeKeyForTFIDF> {

    /* Fields: term     A term (or word)
     *         docID    Document ID for occurence of a term
     *         dfEntry  Flag used to note special record types
     */

        public Text term = new Text(),
                docID = new Text();

        /* dfEntry marked true for records emitted to calculate
           document frequency.  These sort before all other entries
           with the same (docID, term) pair. */
        public BooleanWritable dfEntry = new BooleanWritable();

        /* empty constructor required for serialization */
        public CompositeKeyForTFIDF() {
        }

        public CompositeKeyForTFIDF(String term) {
            this.term.set(term);
            this.docID.set("");
            this.dfEntry.set(true);
        }
        
        public CompositeKeyForTFIDF(String term, String docID, boolean dfEntry) {
            this.term.set(term);
            this.docID.set(docID);
            this.dfEntry.set(dfEntry);
        }

        public String getTerm() {
            return this.term.toString();
        }

        public String getDocID() {
            return this.docID.toString();
        }

        public boolean getDfEntry() {
            return this.dfEntry.get();
        }

        public void write(DataOutput out) throws IOException {
            term.write(out);
            docID.write(out);
            dfEntry.write(out);
        }

        public void readFields(DataInput in) throws IOException {
            term.readFields(in);
            docID.readFields(in);
            dfEntry.readFields(in);
        }

        public int compareTo(CompositeKeyForTFIDF other) {

      /* Compare keys first by term,
         then by dfEntry (true < false),
         then by docID.
      */

            int ret = term.compareTo(other.term);
            if (ret == 0) {
        /* For this field, true < false. */
                ret = -1 * dfEntry.compareTo(other.dfEntry);
            }
            if (ret == 0) {
                ret = docID.compareTo(other.docID);
            }
            return ret;
        }

        public String toString() {
            return "(" + term + "," + docID + "," + dfEntry + ")";
        }

        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            CompositeKeyForTFIDF other = (CompositeKeyForTFIDF) obj;
            if (term == null) {
                if (other.term != null)
                    return false;
            } else if (!term.equals(other.term))
                return false;
            if (docID == null) {
                if (other.docID != null)
                    return false;
            } else if (!docID.equals(other.docID))
                return false;
            if (dfEntry == null) {
                if (other.dfEntry != null)
                    return false;
            } else if (!dfEntry.equals(other.dfEntry))
                return false;
            return true;
        }

        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((term == null) ? 0 : term.hashCode());
            result = prime * result + ((docID == null) ? 0 : docID.hashCode());
            result = prime * result + ((dfEntry == null) ? 0 : dfEntry.hashCode());
            return result;
        }
}
