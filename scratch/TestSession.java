import io.gatling.core.session.Session;
import io.gatling.commons.stats.Status;
public class TestSession {
    public void test(Session session) {
        Session s = session.enterGroup("ACK Group", System.currentTimeMillis());
        s.exitGroup(Status.apply("OK"), System.currentTimeMillis());
    }
}
