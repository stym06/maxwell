package com.olacabs.dp.mail;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.io.UnsupportedEncodingException;
import java.util.Properties;

/**
 * This class is used to send out email Alerts
 *
 * @author abhijit.singh
 * @version 1.0
 * @Date 15/06/16
 */

public class MailHandler {

    private Properties props = new Properties();
    private String fromUserEmail = "Maxwell";
    private String fromUserName ="no-reply@olacabs.com";
    private static MailHandler mailHandler;
    private static final Logger LOGGER = LoggerFactory.getLogger(MailHandler.class);

    private MailHandler(String fromUserName, String fromUserEmail, String auth, String host, String port) {

        if(fromUserName != null)
            this.fromUserName = fromUserName;

        if(fromUserEmail != null)
            this.fromUserEmail = fromUserEmail;

        props.put("mail.smtp.auth", auth);
        props.put("mail.smtp.host", host);
        props.put("mail.smtp.port", port);

        LOGGER.info("MailHandler new instance created for User : {} ", fromUserName);
    }

    public static  MailHandler getInstance(String fromUserName, String fromUserEmail, String auth, String host, String port){
        synchronized(MailHandler.class) {
            if (mailHandler == null)
                mailHandler = new MailHandler(fromUserName, fromUserEmail, auth, host, port);
        }
        return mailHandler;
    }


    public void sendMail(String toAddress, String sub, String msg) {

        Session session = Session.getDefaultInstance(props);

        try {

            Message message = new MimeMessage(session);
            try {
                message.setFrom(new InternetAddress(fromUserName,fromUserEmail));
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            String recipients[] = toAddress.split(",");
            for(int i=0;i<recipients.length;i++)
                message.addRecipient(Message.RecipientType.TO,
                        new InternetAddress(recipients[i]));


            message.setRecipients(Message.RecipientType.TO,
                    InternetAddress.parse(toAddress));
            message.setSubject(sub);
            message.setContent("<h1>"+msg+"</h1>", "text/html");

            Transport.send(message);

            LOGGER.info("Email Sent successfully to : {}, sub : {}, body :{} ", toAddress, sub, msg);

        } catch (MessagingException e) {
            LOGGER.error("{}", e);
        }

    }

    public String compose()
    {
        return null;
    }

    public static void main(String[] args) {

        MailHandler m = MailHandler.getInstance("no-reply@olacabs.com", "Foster Notification Service", "false", "127.0.0.1", "16004");

        StringBuilder sb = new StringBuilder();

        sb.append("test");

        m.sendMail("rajneesh.agrawal@olacabs.com", "test msg", sb.toString());
    }

}
