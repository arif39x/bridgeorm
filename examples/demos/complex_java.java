import io.bridgeorm.core.BridgeORM;
import io.bridgeorm.core.Session;
import java.util.Map;
import java.util.UUID;

public class ComplexJavaDemo {
    public static void main(String[] args) {
        BridgeORM.connect("sqlite:complex_demo.db");

        try (Session session = BridgeORM.beginSession()) {
            AuditLog log = new AuditLog();
            log.setId(UUID.randomUUID());
            log.setAction("COMPLEX_JAVA_INSERT");
            // Map as JSON
            log.setDetails(Map.of(
                "platform", "JVM",
                "thread", Thread.currentThread().getName(),
                "mem_mb", Runtime.getRuntime().totalMemory() / 1024 / 1024
            ));
            
            session.save(log);
            System.out.println("Java: Complex AuditLog saved with ID: " + log.getId());
        }
    }
}
