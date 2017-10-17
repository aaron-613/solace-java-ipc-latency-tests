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
import java.util.logging.Level;

import com.solacesystems.solclientj.core.SolEnum;
import com.solacesystems.solclientj.core.Solclient;
import com.solacesystems.solclientj.core.SolclientException;
import com.solacesystems.solclientj.core.event.MessageCallback;
import com.solacesystems.solclientj.core.event.SessionEventCallback;
import com.solacesystems.solclientj.core.handle.ContextHandle;
import com.solacesystems.solclientj.core.handle.Handle;
import com.solacesystems.solclientj.core.handle.IPCSupport;
import com.solacesystems.solclientj.core.handle.MessageHandle;
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
public class IPC_PerfPub extends AbstractSample {

	private SessionHandle sessionHandle = Solclient.Allocator.newSessionHandle();
	private ContextHandle contextHandle = Solclient.Allocator.newContextHandle();
	private MessageHandle txMessageHandle = Solclient.Allocator.newMessageHandle();
	private Topic topic = Solclient.Allocator.newTopic("aaa");//SampleUtils.SAMPLE_TOPIC);
	private NativeDestinationHandle topicHandle = Solclient.Allocator.newNativeDestinationHandle();
	private int numOfMessages = 100000;
	private int msgSize = 100;
    private int messageRate = 10000;
    private boolean ipcThreadSpinProperty = false;
	private ByteBuffer byteBuffer;
	boolean useDirectByteBuffer = false;

	@Override
	protected void printUsage(boolean secureSession) {
		String usage = ArgumentsParser.getCommonUsage(secureSession);
		System.out.println(usage);
		// extra parameters
		System.out.println("\t -n messages     number messages to send [default "+ numOfMessages + "]");
        System.out.println("\t -r messageRate  how many messages per second [default "+ messageRate + "]");
        System.out.println("\t -s messagesize  message size to publish [default "+ msgSize + "]");
        System.out.println("\t -ts             IPC shared memory thread spin hard, otherwise blocking [default:"+ ipcThreadSpinProperty + "]");
		System.out.println("\t -d              use direct allocate ByteBuffer [default:"+ useDirectByteBuffer + "]");
	}

	/**
	 * This is the main method of the sample
	 */
	@Override
	protected void run(String[] args, SessionConfiguration config, Level logLevel) throws SolclientException {
		// parse the extra parameters
		Map<String, String> cmdLineArgs = config.getArgBag();
		if (cmdLineArgs.containsKey("-n")) {
			numOfMessages = Integer.parseInt(cmdLineArgs.get("-n"));
		}
		if (cmdLineArgs.containsKey("-s")) {
			msgSize = Integer.parseInt(cmdLineArgs.get("-s"));
		}
        if (cmdLineArgs.containsKey("-r")) {
            messageRate = Integer.parseInt(cmdLineArgs.get("-r"));
        }
        if (cmdLineArgs.containsKey("-ts")) {
            ipcThreadSpinProperty = true;
        }
		// Use direct ByteBuffer
		if (cmdLineArgs.containsKey("-d")) {
			useDirectByteBuffer = true;
		}
		if (useDirectByteBuffer) {
			byteBuffer = ByteBuffer.allocateDirect(msgSize);
		} else {
			byteBuffer = ByteBuffer.allocate(msgSize);
	    }
		// Init
		System.out.println(" Initializing the Java RTO Messaging API...");
		int rc = Solclient.init(new String[0]);
		assertReturnCode("Solclient.init()",rc,SolEnum.ReturnCode.OK);
		// We don't care for any output unless it is an error
		Solclient.setLogLevel(Level.INFO);
		// Context
		System.out.println(" Creating a context ...");
        rc = Solclient.createContextForHandle(contextHandle,ipcThreadSpinProperty ?
                new String[]{IPCSupport.CONTEXT_PROPERTIES.IPC_SHM_SPIN,"-1","CONTEXT_CREATE_THREAD","1"} :
                new String[0]);
		assertReturnCode("Solclient.createContext()",rc,SolEnum.ReturnCode.OK);
		// Session
		System.out.println(" Creating a session ...");

		
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
		assertReturnCode("sessionHandle.connect()", rc, SolEnum.ReturnCode.OK);
		// Allocate the message
		rc = Solclient.createMessageForHandle(txMessageHandle);
		assertReturnCode("Solclient.createMessage()", rc, SolEnum.ReturnCode.OK);
		// Allocate a Native Topic Destination
		rc = Solclient.createNativeDestinationForHandle(topicHandle, topic);
        assertReturnCode("createNativeDestinationForHandle()",rc,SolEnum.ReturnCode.OK);
        try {
            Thread.sleep(100);  // this prevents a race condition with the subscriber so that it has time to finish connecting and subscribe before this guy start publishing
        } catch (InterruptedException e) {
        }
		System.out.printf("%nWill publish %d messages of size %d at a rate of %d msg/s using a %s ByteBuffer%n",
				numOfMessages,msgSize,messageRate,useDirectByteBuffer?"DirectAllocated":"ArrayBacked");
		long startTime = System.currentTimeMillis();
        long startOfNext = System.nanoTime();
		long time;
		int messagesSent = 0;
		long delay = 1000000000/messageRate;
		while (messagesSent < numOfMessages) {
            time = System.nanoTime();
            if (time > startOfNext) {
    			txMessageHandle.setDestination(topicHandle);
    			if (msgSize > 0) {
    				byteBuffer.clear();
    				// Making up some pay-load ...
    				byteBuffer.putLong(time);
    				// Fill the byte buffer, using int ( 4 bytes )
    				for (int x = 2; x < msgSize / 4; x++) {
    					byteBuffer.putInt(x);
    				}
    				// Top up with bytes
    				int remainder = msgSize % 4;
    				for (byte b = 0; b < remainder; b++) {
    					byteBuffer.put(b);
    				}
    				byteBuffer.flip();
    				txMessageHandle.setBinaryAttachment(byteBuffer);
    			}
    			rc = sessionHandle.send(txMessageHandle);
    			messagesSent++;
    			startOfNext += delay;  // busy wait
            }
		}
		long elapsedMs = System.currentTimeMillis()-startTime;
		double txRate = (double) numOfMessages / (double) elapsedMs;
		System.out.printf("%nSent %d messages in %f seconds = %f msg/second (calculated)%n%n",numOfMessages,elapsedMs/1000f,txRate*1000);
		try {
            Thread.sleep(100);  // just to make sure the receiving application gets all
        } catch (InterruptedException e) {
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

	static class CustomEventsAdapter implements MessageCallback,SessionEventCallback {

		@Override
		public void onEvent(SessionHandle sessionHandle) {
		}

		@Override
		public void onMessage(Handle handle) {
		}

	}

/**
     * Boilerplate, calls {@link #run(String[])
     * @param args
     */
	public static void main(String[] args) {
		IPC_PerfPub sample = new IPC_PerfPub();
		sample.run(args);
	}

}
