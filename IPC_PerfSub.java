/**
 * Copyright 2004-2017 Solace Corporation. All rights reserved.
 *
 */
package com.solace.aa.javarto;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import com.solacesystems.solclientj.core.SolEnum;
import com.solacesystems.solclientj.core.SolEnum.SessionEventCode;
import com.solacesystems.solclientj.core.Solclient;
import com.solacesystems.solclientj.core.SolclientException;
import com.solacesystems.solclientj.core.event.MessageCallback;
import com.solacesystems.solclientj.core.event.SessionEventCallback;
import com.solacesystems.solclientj.core.handle.ContextHandle;
import com.solacesystems.solclientj.core.handle.Handle;
import com.solacesystems.solclientj.core.handle.IPCSupport;
import com.solacesystems.solclientj.core.handle.MessageHandle;
import com.solacesystems.solclientj.core.handle.MessageSupport;
import com.solacesystems.solclientj.core.handle.NativeDestinationHandle;
import com.solacesystems.solclientj.core.handle.SessionHandle;
import com.solacesystems.solclientj.core.resource.Topic;
import com.solacesystems.solclientj.core.samples.common.AbstractSample;
import com.solacesystems.solclientj.core.samples.common.ArgumentsParser;
import com.solacesystems.solclientj.core.samples.common.SampleUtils;
import com.solacesystems.solclientj.core.samples.common.SessionConfiguration;

/**
 * 
 * PerfPubSub.java
 * 
 * This sample is a GC-free performance test that demonstrates:
 * <ul>
 * <li>Subscribing to a topic for direct messages.
 * <li>Publishing direct messages to a topic.
 * <li>Session Event are ignored, Message Events ignored.
 * <ul>
 * 
 */
public class IPC_PerfSub extends AbstractSample {

	private SessionHandle sessionHandle = Solclient.Allocator.newSessionHandle();
	private ContextHandle contextHandle = Solclient.Allocator.newContextHandle();
	private MessageHandle txMessageHandle = Solclient.Allocator.newMessageHandle();
	private Topic topic = Solclient.Allocator.newTopic("aaa");//SampleUtils.SAMPLE_TOPIC);
	private NativeDestinationHandle topicHandle = Solclient.Allocator.newNativeDestinationHandle();
	private static int numReceived = 0;  // total
    private static ByteBuffer byteBuffer;
    private static byte[] localCopy = new byte[10000];  // could be big array
    static {
        Arrays.fill(localCopy,(byte)32);  // fill with stuff... payloadSize amount will get overwritten
    }
    private static int payloadSize = 0;
	private boolean useDirectByteBuffer = false;
	private static long[] sentTs = new long[30*300000];  // max number of measurements... 30 sec at 300k msg/s
    private static long[] receivedTs = new long[30*300000];
    private int bucketSize = 1000;  // nanoseconds
    private int bucketNum = 10;
    private boolean ipcThreadSpinProperty = false;
    private static CountDownLatch latch = new CountDownLatch(2);  // one for UP_NOTICEE, one for "disconnect" or "RECONNECTING" when publisher goes offline

	@Override
	protected void printUsage(boolean secureSession) {
		String usage = ArgumentsParser.getCommonUsage(secureSession);
		System.out.println(usage);
		// extra parameters
//		System.out.println("\t -n messages     number messages to receive [default "+ numOfMessages + "]");
		System.out.println("\t -d              use direct allocate ByteBuffer [default:"+ useDirectByteBuffer + "]");
        System.out.println("\t -ts             IPC shared memory thread spin hard, otherwise blocking [default:"+ ipcThreadSpinProperty + "]");
        System.out.println("\t -lg bucketSize  how large each bucket is in nanoseconds [default:"+ bucketSize + "]");
        System.out.println("\t -lb numBuckets  how many buckets to use [default:"+ bucketNum + "]");
	}

	//-h listen://127.0.0.1 -u test -n 100000 -s 100 -d true
	
	/**
	 * This is the main method of the sample
	 */
	@Override
	protected void run(String[] args, SessionConfiguration config, Level logLevel) throws SolclientException {
		// parse the extra parameters
		Map<String, String> cmdLineArgs = config.getArgBag();
		if (cmdLineArgs.containsKey("-d")) {
		    useDirectByteBuffer = true;
            byteBuffer = ByteBuffer.allocateDirect(10000);
		} else {
			byteBuffer = ByteBuffer.allocate(10000);
		}
        if (cmdLineArgs.containsKey("-ts")) {
            ipcThreadSpinProperty = true;
        }
        if (cmdLineArgs.containsKey("-lg")) {
            bucketSize = Integer.parseInt(cmdLineArgs.get("-lg"));
        }
        if (cmdLineArgs.containsKey("-lb")) {
            bucketNum = Integer.parseInt(cmdLineArgs.get("-lb"));
        }
        System.out.printf("%nWill subscribe to receive messages into %d buckets of %d ns, using a %s ByteBuffer%n%n",bucketNum,bucketSize,useDirectByteBuffer?"DirectAllocated":"ArrayBacked");
	
		// Init
		System.out.println(" Initializing the Java RTO Messaging API...");
		int rc = Solclient.init(new String[0]);
		assertReturnCode("Solclient.init()", rc, SolEnum.ReturnCode.OK);
		// We don't care for any output unless it is an error
		Solclient.setLogLevel(Level.INFO);
		// Context
		System.out.println(" Creating a context ...");
		rc = Solclient.createContextForHandle(contextHandle,ipcThreadSpinProperty ?
                new String[]{IPCSupport.CONTEXT_PROPERTIES.IPC_SHM_SPIN,"-1","CONTEXT_CREATE_THREAD","1"} :
                new String[0]);
		assertReturnCode("Solclient.createContext()", rc, SolEnum.ReturnCode.OK);
		// Session
		System.out.println(" Creating a session ...");

//		String[] sessionProps = getSessionProps(config, 0);
        String[] sessionPropsOrig = getSessionProps(config, 0);
        // Aaron's mods
        List<String> sessionProperties = new ArrayList<String>(Arrays.asList(sessionPropsOrig));
        sessionProperties.add(IPCSupport.SESSION_PROPERTIES.MULTIPOINT);
        sessionProperties.add(SolEnum.BooleanValue.ENABLE);
        String[] sessionProps;
        sessionProps = sessionProperties.toArray(new String[0]);

		CustomEventsAdapter adapter = new CustomEventsAdapter();
		rc = contextHandle.createSessionForHandle(sessionHandle,sessionProps,adapter,adapter);
		assertReturnCode("contextHandle.createSession()",rc,SolEnum.ReturnCode.OK);
		System.out.println("Session Properties: "+Arrays.toString(sessionProps));
		// Connect
		System.out.println(" Connecting session ...");
		rc = sessionHandle.connect();
		assertReturnCode("sessionHandle.connect()",rc,SolEnum.ReturnCode.OK);
		// Subscribe
		System.out.println(" Adding subscription ...");
		rc = sessionHandle.subscribe(topic,SolEnum.SubscribeFlags.WAIT_FOR_CONFIRM, 0);
		assertReturnCode("sessionHandle.subscribe()",rc,SolEnum.ReturnCode.OK);
		// Allocate a Native Topic Destination
		rc = Solclient.createNativeDestinationForHandle(topicHandle, topic);
        assertReturnCode("createNativeDestinationForHandle()",rc,SolEnum.ReturnCode.OK);

		try {
	        latch.await(90,TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            System.err.println("PerfSub got interrupted");
            System.out.println("** DONE **************************************");
		}

	    System.out.printf("%nReceived %d total messages.  Calculating buckets using 2nd half of measurements...%n%n",numReceived);
		int[] buckets = new int[bucketNum];
		Arrays.fill(buckets,0);
		int over = 0;  // anything outside bucket range
        long min = receivedTs[0]-sentTs[0];
        long max = min;
        long total = 0;
        double sentT = (sentTs[numReceived-1]-sentTs[0])/1000000000.0;
        long rate = Math.round(numReceived / sentT);
		for (int i=numReceived/2;i<numReceived;i++) {  // throw away the first half of measurements (the warm-up)
		    long delta = receivedTs[i]-sentTs[i];  // e.g. 8234ns = 8.2us
		    total += delta;
		    if (delta < min) min = delta;
		    if (delta > max) max = delta;
		    if (delta >= (bucketSize*bucketNum)) {
		        over++;
		    } else {
		        buckets[(int)(delta/bucketSize)]++;
		    }
		    sentTs[i] = delta;  // overwrite it, and keep for later
		}
		receivedTs = new long[numReceived/2];
		for (int i=0;i<numReceived/2;i++) {
		    receivedTs[i] = sentTs[(numReceived/2)+i];  // use the delta that overwrote the sentTs above
		}
		Arrays.sort(receivedTs);  // sort the whole thing, so that we can find the various percentiles
        if (cmdLineArgs.containsKey("-csv")) {
            System.out.printf("### Rate,Size,Allocate Direct,Thread Spin,\"Min, %db, %s, %s\",\"50th%%, %db, %s, %s\",\"95th%%, %db, %s, %s\",\"99th%%, %db, %s, %s\",\"99.5th%%, %db, %s, %s\",\"99.9th%%, %db, %s, %s\"%n",
                    payloadSize,useDirectByteBuffer ? "Direct Buffer" : "Non-Direct Buffer",ipcThreadSpinProperty ? "Thread Spin" : "Thread Blocking",  // min
                    payloadSize,useDirectByteBuffer ? "Direct Buffer" : "Non-Direct Buffer",ipcThreadSpinProperty ? "Thread Spin" : "Thread Blocking",  // 50th %
                    payloadSize,useDirectByteBuffer ? "Direct Buffer" : "Non-Direct Buffer",ipcThreadSpinProperty ? "Thread Spin" : "Thread Blocking",  // 95th
                    payloadSize,useDirectByteBuffer ? "Direct Buffer" : "Non-Direct Buffer",ipcThreadSpinProperty ? "Thread Spin" : "Thread Blocking",  // 99th
                    payloadSize,useDirectByteBuffer ? "Direct Buffer" : "Non-Direct Buffer",ipcThreadSpinProperty ? "Thread Spin" : "Thread Blocking",  // 99.5th
                    payloadSize,useDirectByteBuffer ? "Direct Buffer" : "Non-Direct Buffer",ipcThreadSpinProperty ? "Thread Spin" : "Thread Blocking"); // 99.9th
            System.out.printf("### %d,%d,%b,%b,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f%n",
                    rate,
                    payloadSize,
                    useDirectByteBuffer,
                    ipcThreadSpinProperty,
                    min/1000f,
                    receivedTs[(int)((numReceived/2)*0.5)]/1000f,
                    receivedTs[(int)((numReceived/2)*0.95)]/1000f,
                    receivedTs[(int)((numReceived/2)*0.99)]/1000f,
                    receivedTs[(int)((numReceived/2)*0.995)]/1000f,
                    receivedTs[(int)((numReceived/2)*0.999)]/1000f);
            System.out.println();
        } else {
            int cumulative = 0;
            for (int i=0;i<bucketNum;i++) {
                cumulative += buckets[i];
                System.out.printf("%2d) [%5.2fus-%5.2fus] %7d  |  %5.2f%%%n",i,(bucketSize*i)/1000f,((bucketSize*(i+1))-1)/1000f,buckets[i],(cumulative*100.0)/(numReceived/2));
            }
            System.out.printf("%2d)        > %5.2fus  %7d  |  %4.3f%%%n",bucketNum,((bucketSize*(bucketNum))-1)/1000f,over,(over*100.0)/(numReceived/2));
            System.out.println("=====================");
            System.out.printf("      Min: %7.2fus%n",min/1000f);
            //System.out.printf("      Avg: %7.2fus%n",(total/(numReceived/2))/1000f);
            System.out.printf("    50th%%: %7.2fus%n",receivedTs[(int)((numReceived/2)*0.5)]/1000f);
            System.out.printf("    95th%%: %7.2fus%n",receivedTs[(int)((numReceived/2)*0.95)]/1000f);
            System.out.printf("    99th%%: %7.2fus%n",receivedTs[(int)((numReceived/2)*0.99)]/1000f);
            System.out.printf("  99.5th%%: %7.2fus%n",receivedTs[(int)((numReceived/2)*0.995)]/1000f);
            System.out.printf("  99.9th%%: %7.2fus%n",receivedTs[(int)((numReceived/2)*0.999)]/1000f);
            System.out.printf("      Max: %7.2fus%n",max/1000f);
            System.out.println();
        }
	}

	/**
	 * Invoked when the sample finishes
	 */
	@Override
	protected void finish(int status) {
		/*************************************************************************
		 * Cleanup
		 *************************************************************************/
		finish_DestroyHandle(txMessageHandle, "messageHandle");
		finish_Disconnect(sessionHandle);
		finish_DestroyHandle(sessionHandle, "sessionHandle");
		finish_DestroyHandle(contextHandle, "contextHandle");
		finish_Solclient();
	}

	static class CustomEventsAdapter implements MessageCallback, SessionEventCallback {

		@Override
		public void onEvent(SessionHandle sessionHandle) {
            System.out.printf("*** Received an Event! %s - %s%n",SessionEventCode.toString(sessionHandle.getSessionEvent().getSessionEventCode()),sessionHandle.getSessionEvent().getInfo());
            latch.countDown();
		}

		@Override
		public void onMessage(Handle handle) {
		    try {
    		    receivedTs[numReceived] = System.nanoTime();
                MessageSupport messageSupport = (MessageSupport)handle;
                messageSupport.getRxMessage().getBinaryAttachment(byteBuffer);
                byteBuffer.flip();
                payloadSize = byteBuffer.limit();
                sentTs[numReceived] = byteBuffer.getLong();
                byteBuffer.get(localCopy,0,byteBuffer.limit()-byteBuffer.position());  // this is to be more accurate... actually read the payload into the JVM
                numReceived++;
                byteBuffer.clear();
		    } catch (Exception e) {
		        e.printStackTrace();
		        latch.countDown();
		    }
		}
	}

/**
     * Boilerplate, calls {@link #run(String[])
     * @param args
     */
	public static void main(String[] args) {
		IPC_PerfSub sample = new IPC_PerfSub();
		sample.run(args);
	}

}
