package cassdemo;

import java.util.List;
import java.util.Map;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

public class ToStringer {
    public static String proposalToString(Row row){
        StringBuilder sb = new StringBuilder();
        sb.append(Integer.toString(row.getInt("student_id")));
        sb.append("\t\t");
        sb.append(row.getString("list_name"));
        sb.append("\t");
        //problem in datastax codec
        //sb.append(row.getDate("sending_time"));
        //sb.append("\t");
        List<Integer> placements = row.getList("placements", Integer.class);
        sb.append("[ ");
        for(int placement:placements){
            sb.append(Integer.toString(placement)+" ");
        } 
        sb.append("]");
        return sb.toString();
    }

    public static String proposalsToString(ResultSet rs){
        StringBuilder sb = new StringBuilder();
        sb.append("student_id\tlist_name\tplacements\n");
        //sb.append("student_id\tlist_name\tsending_time\tplacements\n");
        for(Row proposal: rs){
            sb.append(proposalToString(proposal)+"\n");
        }
        return sb.toString();
    }

    public static String listToString(Row row){
        StringBuilder sb = new StringBuilder();
        sb.append("Name: "+row.getString("name")+", ");
        sb.append("max_size: "+row.getInt("max_size")+"\n");
        sb.append("position\tstudent_id\n");
        Map<Integer, Integer> students=row.getMap("students", Integer.class, Integer.class);
        for(int key:students.keySet()){
            sb.append(Integer.toString(key)+ "\t\t"+Integer.toString(students.get(key))+"\n");
        }
        return sb.toString();
    }

    public static String listsToString(ResultSet rs){
        StringBuilder sb = new StringBuilder();
        for(Row row: rs){
            sb.append(listToString(row)+"\n");
        }
        return sb.toString();
    }
}
