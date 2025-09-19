import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.ClusterConnectionConfiguration;
import org.apache.activemq.artemis.core.config.CoreAddressConfiguration;
import org.apache.activemq.artemis.core.config.WildcardConfiguration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.jms.client.ActiveMQConnection;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.jms.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class DefaultWildcardTest extends ActiveMQTestBase implements MessageListener {


   ActiveMQServer server1;
   ActiveMQServer server2;
   CountDownLatch countDownLatch = new CountDownLatch(1);
   private static final String address = "Queue";
   private static final String queue = "Queue";
   private static final String fqqn = address + "::" + queue;
   public void startServer() throws Exception {

   }

   @BeforeClass
   public static void beforeClass() throws Exception {
     // Assume.assumeTrue(CheckLeak.isLoaded());
   }

   @Override
   @Before
   public void setUp() throws Exception {
      startServer();
   }

   @Override
   public void tearDown() throws Exception {
      super.tearDown();
      server1 = null;
      server2 = null;
   }

   @Test
   public void testRedistributor() throws Exception {

      HashMap<String, Object> map = new HashMap<String, Object>();
      map.put("host", "localhost");
      map.put("port", 61616);
      HashMap<String, Object> map2 = new HashMap<String, Object>();
      map2.put("host", "localhost");
      map2.put("port", 61617);

/*
      WildcardConfiguration wildcardConfiguration = new WildcardConfiguration();
      wildcardConfiguration.setDelimiter('.');
      wildcardConfiguration.setSingleWord('*');
      wildcardConfiguration.setAnyWords('#');
*/


      QueueConfiguration queueConfiguration1 = new QueueConfiguration();
      queueConfiguration1.setName("TEST");
      queueConfiguration1.setEnabled(true);
      queueConfiguration1.setDurable(true);
      queueConfiguration1.setAddress("TEST");
      queueConfiguration1.setRoutingType(RoutingType.ANYCAST);
      queueConfiguration1.setDurable(true);

       List<QueueConfiguration> queueCofig1 = new ArrayList<>();
      queueCofig1.add(queueConfiguration1);



       QueueConfiguration queueConfiguration2 = new QueueConfiguration();
       queueConfiguration2.setName("DLQ");
       queueConfiguration2.setEnabled(true);
       queueConfiguration2.setDurable(true);
       queueConfiguration2.setAddress("DLQ");
       queueConfiguration2.setRoutingType(RoutingType.ANYCAST);
       queueConfiguration2.setDurable(true);

       List<QueueConfiguration> queueCofig2= new ArrayList<>();
       queueCofig2.add(queueConfiguration2);

       QueueConfiguration queueConfiguration3 = new QueueConfiguration();
       queueConfiguration3.setName("ExpiryQueue");
       queueConfiguration3.setEnabled(true);
       queueConfiguration3.setDurable(true);
       queueConfiguration3.setAddress("ExpiryQueue");
       queueConfiguration3.setRoutingType(RoutingType.ANYCAST);
       queueConfiguration3.setDurable(true);

       List<QueueConfiguration> queueCofig3= new ArrayList<>();
        queueCofig3.add(queueConfiguration3);

       QueueConfiguration queueConfiguration4 = new QueueConfiguration();
       queueConfiguration4.setName("log");
       queueConfiguration4.setEnabled(true);
       queueConfiguration4.setDurable(true);
       queueConfiguration4.setAddress("log");
       queueConfiguration4.setRoutingType(RoutingType.ANYCAST);
       queueConfiguration4.setDurable(true);

       List<QueueConfiguration> queueCofig4= new ArrayList<>();
        queueCofig4.add(queueConfiguration4);

        Set<String> routingTypeSet =  new HashSet<>();
       routingTypeSet.add(RoutingType.ANYCAST.toString());

       CoreAddressConfiguration coreAddressConfiguration1 = new CoreAddressConfiguration();
       coreAddressConfiguration1.setQueueConfigs(queueCofig1);
       coreAddressConfiguration1.setName("TEST");
       coreAddressConfiguration1.setRoutingTypes(routingTypeSet);

       CoreAddressConfiguration coreAddressConfiguration2 = new CoreAddressConfiguration();
       coreAddressConfiguration2.setQueueConfigs(queueCofig2);
       coreAddressConfiguration2.setName("DLQ");
       coreAddressConfiguration2.setRoutingTypes(routingTypeSet);

       CoreAddressConfiguration coreAddressConfiguration3 = new CoreAddressConfiguration();
       coreAddressConfiguration3.setQueueConfigs(queueCofig3);
       coreAddressConfiguration3.setName("ExpiryQueue");
       coreAddressConfiguration3.setRoutingTypes(routingTypeSet);

       CoreAddressConfiguration coreAddressConfiguration4 = new CoreAddressConfiguration();
       coreAddressConfiguration4.setQueueConfigs(queueCofig4);
       coreAddressConfiguration4.setName("log");
       coreAddressConfiguration4.setRoutingTypes(routingTypeSet);


       ClusterConnectionConfiguration clusterConnectionConfiguration = new ClusterConnectionConfiguration().
              setName("my-cluster").setConnectorName("local").
              setInitialConnectAttempts(-1).setReconnectAttempts(-1)
              .setDuplicateDetection(true).setMaxHops(1)
              .setMessageLoadBalancingType(MessageLoadBalancingType.ON_DEMAND).
              setStaticConnectors(List.of("remote"));


      ConfigurationImpl config1 =  createBasicConfig(0);
       config1.setName("broker1");
      config1.getConnectorConfigurations().put("local", new TransportConfiguration(NETTY_CONNECTOR_FACTORY,map));
      config1.getConnectorConfigurations().put("remote", new TransportConfiguration(NETTY_CONNECTOR_FACTORY,map2));
      config1.getAcceptorConfigurations().add(new TransportConfiguration(NETTY_ACCEPTOR_FACTORY,map));
      //config1.setWildCardConfiguration(wildcardConfiguration);

      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setRedistributionDelay(0);
      addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);
      addressSettings.setAutoCreateAddresses(true);
      addressSettings.setAutoDeleteAddresses(false);
      addressSettings.setAutoCreateQueues(true);
      addressSettings.setAutoDeleteQueues(false);

      server1 = createServer(false, config1);
      server1.getConfiguration().addAddressSetting("#", addressSettings);
      server1.getConfiguration().addAddressSetting("activemq.management#", addressSettings);
      server1.getConfiguration().addClusterConfiguration(clusterConnectionConfiguration);
      server1.getConfiguration().setClusterUser("admin");
      server1.getConfiguration().setClusterPassword("admin");
      server1.getConfiguration().setPersistenceEnabled(true);
      server1.getConfiguration().setJournalDirectory("target/journal1");
      server1.getConfiguration().setBindingsDirectory("target/bindings1");
      server1.getConfiguration().setLargeMessagesDirectory("target/lm1");
      server1.getConfiguration().setPagingDirectory("target/pg1");
      server1.getConfiguration().addAddressConfiguration(coreAddressConfiguration1);
      server1.getConfiguration().addAddressConfiguration(coreAddressConfiguration2);
      server1.getConfiguration().addAddressConfiguration(coreAddressConfiguration3);
      server1.getConfiguration().addAddressConfiguration(coreAddressConfiguration4);
      server1.start();


      ConfigurationImpl config2 =  createBasicConfig(1);
       config2.setName("broker2");
      config2.getConnectorConfigurations().put("remote", new TransportConfiguration(NETTY_CONNECTOR_FACTORY,map));
      config2.getConnectorConfigurations().put("local", new TransportConfiguration(NETTY_CONNECTOR_FACTORY,map2));
      config2.getAcceptorConfigurations().add(new TransportConfiguration(NETTY_ACCEPTOR_FACTORY,map2));
      //config2.setWildCardConfiguration(wildcardConfiguration);

      server2 = createServer(false, config2);
      server2.getConfiguration().addAddressSetting("#", addressSettings);
       server1.getConfiguration().addAddressSetting("activemq.management#", addressSettings);
      server2.getConfiguration().addClusterConfiguration(clusterConnectionConfiguration);
      server2.getConfiguration().setClusterUser("admin");
      server2.getConfiguration().setClusterPassword("admin");
      server2.getConfiguration().setPersistenceEnabled(true);
       server2.getConfiguration().setJournalDirectory("target/journal2");
       server2.getConfiguration().setBindingsDirectory("target/bindings2");
       server2.getConfiguration().setLargeMessagesDirectory("target/lm2");
       server2.getConfiguration().setPagingDirectory("target/pg2");

       server2.getConfiguration().addAddressConfiguration(coreAddressConfiguration1);
       server2.getConfiguration().addAddressConfiguration(coreAddressConfiguration2);
       server2.getConfiguration().addAddressConfiguration(coreAddressConfiguration3);
       server2.getConfiguration().addAddressConfiguration(coreAddressConfiguration4);
      server2.start();


      Thread.sleep(1000);


       ActiveMQConnectionFactory consumerFactory = new ActiveMQConnectionFactory("(tcp://localhost:61616)?failoverOnInitialConnection=true&reconnectAttempts=20&callTimeout=2000&retryInterval=2000");
       Connection connection = consumerFactory.createConnection();
       connection.start();
           for (int i=0; i<1; i++) {
               Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
               Destination dest = session.createQueue("log");
               session.createConsumer(dest).setMessageListener(this);

           }
       System.out.println(">>>>>>>>>>>>>> Consumer Created >>>>>>>>>>>>>>");

       Thread.sleep(500);
       server1.stop();
       Thread.sleep(500);
       server1.start();
       Thread.sleep(500);


       for (int i=0; i<1; i++) {
           Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
           Destination dest = session.createQueue("log");
           session.createConsumer(dest).setMessageListener(this);

       }

       Thread.sleep(500);
       server1.stop();
       Thread.sleep(500);
       server1.start();
       Thread.sleep(500);


       for (int i=0; i<1; i++) {
           Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
           Destination dest = session.createQueue("log");
           session.createConsumer(dest).setMessageListener(this);

       }


       ((ActiveMQConnection)connection).getSessionFactory().getConnection().destroy();
       System.out.println(">> END >>");
       connection.close();
     server1.stop();
     server2.stop();
     super.tearDown();

      }

   @Override
   public void onMessage(Message message) {
      System.out.println(">> Async Consumer Received The Message >> "+message);
   }
}
