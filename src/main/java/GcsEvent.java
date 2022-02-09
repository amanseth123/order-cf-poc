

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class GcsEvent {
    private String bucket;
    private String name;
    private String metageneration;
    private Date timeCreated;
    private Date updated;

    @Override
    public String toString() {
        return "GcsEvent [bucket=" + bucket + ", name=" + name + ", metageneration=" + metageneration + ", timeCreated="
                + timeCreated + ", updated=" + updated + "]";
    }

}
