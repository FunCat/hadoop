import org.apache.hadoop.io.Text;

public class LongestWordUtils {

    public static int updateMaxLength(String longestWord) {
        return longestWord.length();
    }

    public static int updateMaxLength(Text longestWord) {
        return longestWord.toString().length();
    }
}
