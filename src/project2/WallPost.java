package project2;
import java.io.Serializable;

public class WallPost implements Serializable {

	private final NodeAddress origin;
	private final String message;
	
	public WallPost(NodeAddress origin, String message) {
		this.origin = origin;
		this.message = message;
	}
	
	public NodeAddress getOrigin() {
		return origin;
	}
	
	public String getMessage() {
		return message;
	}
	
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (obj == this)
            return true;
        if (!(obj instanceof NodeAddress))
            return false;

        WallPost post = (WallPost)obj;
        return this.origin.equals(post.origin) && this.message.equals(post.message);
    }
    
    public int hashCode() {
        return origin.hashCode() + message.hashCode();
    }
}
