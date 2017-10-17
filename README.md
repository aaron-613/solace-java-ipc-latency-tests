# solace-java-ipc-latency-tests

These Java source files can be used to perform latency testing with the IPC-enabled Solace Java Real-Time Optimized (Java RTO) API.  Note that the IPC-enabled API is not available publicly, you must purchase or request a trial licence from Solace.


The standard Java RTO API can found here: http://dev.solace.com/downloads/#apis-protocols-tools , but this one doesn't have the IPC capability unlocked... just connects to the router.

 
The Solace Java RTO API is a JNI-wrapped C API.  It can be used both 


Solace has the SdkPerf test tool in a variety of API flavours, and it can be used to perform latency testing.  However, the Java one uses millisecond resolution, which is not accurate enough when performing IPC latency testing.



