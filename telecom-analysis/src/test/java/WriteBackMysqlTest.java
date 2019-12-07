import com.jj.mapreduce.dto.mapreduce.key.CombineDimension;
import com.jj.mapreduce.dto.mapreduce.key.ContactDimension;
import com.jj.mapreduce.dto.mapreduce.key.DateDimension;
import com.jj.mapreduce.dto.mapreduce.value.AnalysisResult;
import com.jj.mapreduce.outputformat.WritebackMysqlService;
import org.junit.Test;

public class WriteBackMysqlTest {
    @Test
    public void test(){
        WritebackMysqlService service = new WritebackMysqlService();
        CombineDimension combineDimension = CombineDimension.builder()
                .contactDimension(ContactDimension.builder().telephone("12222233333").name("李四").build())
                .dateDimension(DateDimension.builder().year(2018).month(8).day(7).build()).build();
        AnalysisResult analysisResult = AnalysisResult.builder().callSum(100).durationSum(1000).build();
        service.writeMysql(combineDimension,analysisResult);
    }
}
