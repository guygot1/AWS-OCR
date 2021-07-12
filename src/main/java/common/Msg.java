package common;

import com.lowagie.text.pdf.codec.Base64;

import java.io.Serializable;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.InputStream;
import java.io.IOException;


public class Msg implements Serializable {
    private String from;
    private String to;
    private String topic;
    private String body;
    private String info1;
    private String info2;
    private String info3;
    private String attached;

    public Msg(String from, String to, String topic, String body, String info1, String info2, String info3, String attached) {
        this.from = from;
        this.to = to;
        this.topic = topic;
        this.body = body;
        this.info1 = info1;
        this.info2 = info2;
        this.info3 = info3;
        this.attached = attached;
    }

    public String getFrom() {
        return from;
    }

    public String getTo() {
        return to;
    }

    public String getTopic() {
        return topic;
    }

    public String getBody() {
        return body;
    }

    public String getInfo1() {
        return info1;
    }

    public String getInfo2() {
        return info2;
    }

    public String getInfo3() {
        return info3;
    }

    public String getAttached() {
        return attached;
    }

    public static String serialize(Msg message) {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try {
            ObjectOutputStream out = new ObjectOutputStream(buffer);
            out.writeObject(message);
            out.close();
            return Base64.encodeBytes(buffer.toByteArray());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }

    public static Msg deserialize(String text) {
        try {
            InputStream buffer_in = new ByteArrayInputStream(Base64.decode(text));
            ObjectInputStream in = new ObjectInputStream(buffer_in);
            Msg task = (Msg) in.readObject();
            in.close();
            return task;
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

}
