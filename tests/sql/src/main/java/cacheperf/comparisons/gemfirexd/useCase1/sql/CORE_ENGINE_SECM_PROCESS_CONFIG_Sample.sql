SET CURRENT SCHEMA=SEC_OWNER;

INSERT
INTO SECM_PROCESS_CONFIG
  (
    PROCESS_ID, HOSTNAME, ROOT_PATH, MULE_CONFIG, START_COMMAND,
    ONBOARD_DATE, END_DATE, LAST_UPDATE_BY, LAST_UPDATE_DATE
  )
  VALUES
  ('PROC_1','localhost','C:\softwares\mule-standalone-3.3.0','<?xml version="1.0" encoding="UTF-8"?>
<mule xmlns="http://www.mulesoft.org/schema/mule/core" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	xmlns:spring="http://www.springframework.org/schema/beans" 
	xmlns:jms="http://www.mulesoft.org/schema/mule/jms"
	xmlns:test="http://www.mulesoft.org/schema/mule/test" 
	xmlns:vm="http://www.mulesoft.org/schema/mule/vm"
	xmlns:context="http://www.springframework.org/schema/context"
	xmlns:quartz="http://www.mulesoft.org/schema/mule/quartz"
	xmlns:management="http://www.mulesoft.org/schema/mule/management"
	xsi:schemaLocation="
	http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans-3.0.xsd
	http://www.mulesoft.org/schema/mule/core http://www.mulesoft.org/schema/mule/core/3.1/mule.xsd 
	http://www.mulesoft.org/schema/mule/jms http://www.mulesoft.org/schema/mule/jms/3.1/mule-jms.xsd
	http://www.mulesoft.org/schema/mule/test http://www.mulesoft.org/schema/mule/test/3.1/mule-test.xsd
	http://www.mulesoft.org/schema/mule/vm http://www.mulesoft.org/schema/mule/vm/3.1/mule-vm.xsd
	http://www.springframework.org/schema/context http://www.springframework.org/schema/context/spring-context-3.0.xsd
	http://www.mulesoft.org/schema/mule/quartz http://www.mulesoft.org/schema/mule/quartz/3.0/mule-quartz.xsd
	http://www.mulesoft.org/schema/mule/management http://www.mulesoft.org/schema/mule/management/3.1/mule-management.xsd">

	<context:property-placeholder
		location="classpath:securitas.properties, classpath:fsmessagesender.properties, classpath:scheduledjobs-quartz.properties" />

	<spring:beans>

		<spring:import resource="classpath:dao-infra.xml" />
		<spring:import resource="classpath:dao-context.xml" />
		<spring:import resource="classpath:configuration-context.xml" />
		<spring:import resource="classpath:boprocessor-context.xml" />
		<spring:import resource="classpath:fsprocessor-context.xml" />
		<spring:import resource="classpath:matcher-context.xml" />
		<spring:import resource="classpath:fsmessagesender-context.xml" />
		<!-- TODO remove this once initialization problems are resolved
		<spring:import resource="classpath:scheduledjobs-context.xml" />
		 -->

	</spring:beans>


	<!-- Connectors and Endpoints -->

	<!-- ActiveMQ Connection Factory -->
	<jms:activemq-connector name="BO1.MqFac"
		brokerURL="tcp://localhost:61616" />

	<jms:endpoint name="BO1.REQ.IN.QUEUE" queue="IPAY_BO1_INPUT"
		connector-ref="BO1.MqFac" />

	<jms:endpoint name="OFAC1.REQ.OUT.QUEUE" queue="IPAY_OFAC1_INPUT"
		connector-ref="BO1.MqFac" />

	<jms:endpoint name="OFAC1.ACK.IN.QUEUE" queue="IPAY_OFAC1_ACK"
		connector-ref="BO1.MqFac" />

	<jms:endpoint name="BO1.ACK.OUT.QUEUE" queue="IPAY_BO1_ACK"
		connector-ref="BO1.MqFac" />

	<jms:endpoint name="OFAC1.DECISION.IN.QUEUE" queue="IPAY_OFAC1_DECISION"
		connector-ref="BO1.MqFac" />

	<jms:endpoint name="BO1.DECISION.OUT.QUEUE" queue="IPAY_BO1_DECISION"
		connector-ref="BO1.MqFac" />

	<jms:endpoint name="OFAC1.STOP.IN.QUEUE" queue="IPAY_OFAC1_STOP"
		connector-ref="BO1.MqFac" />

	<jms:endpoint name="BO1.STOP.OUT.QUEUE" queue="IPAY_BO1_STOP"
		connector-ref="BO1.MqFac" />


	<!-- Mule scheduler -->
	<!-- TODO remove this once initialization problems are resolved
	<quartz:connector name="quartzConnector" scheduler-ref="quartzSchedulerFactoryBean"/>
	 -->

 
	<!-- Mule Flows -->

	<!-- This VM connector bridges the output flow 1 and the input of flow 2 -->
	<vm:connector name="VmConnector" />

	<!-- Transaction 1
          1. Receive BO message
          2. Persist raw BO message
	-->
	<flow name="BO1.RawMessageProcessorFlow">

		<!-- there will be multiple incoming queues -->
		<composite-source>

			<!-- transaction starts here -->
			<jms:inbound-endpoint ref="BO1.REQ.IN.QUEUE" >
	 			<jms:transaction action="ALWAYS_BEGIN" />
	 		</jms:inbound-endpoint>
			
		</composite-source>

		<component>
			<spring-object bean="rawMessageProcessorImpl" />
		</component>

		<vm:outbound-endpoint path="BO1.PARSE_BOMESSAGE" exchange-pattern="one-way" connector-ref="VmConnector" >
			<vm:transaction action="NONE" />
		</vm:outbound-endpoint>
		
		<!-- explicit definition of rollback max retry and rollback Q-->
		<rollback-exception-strategy maxRedeliveryAttempts="3">
      		<on-redelivery-attempts-exceeded>
		         <jms:outbound-endpoint queue="BO1.REQ.IN.DLQ">
		            <jms:transaction action="ALWAYS_JOIN"/>  
		         </jms:outbound-endpoint>
	        </on-redelivery-attempts-exceeded>
   		</rollback-exception-strategy>
	</flow>


	<!-- Transaction 2 
          1. Persist parsed BO data to BO data table 
	-->
	<flow name="BO1.BOMessageProcessorFlow">
		<vm:inbound-endpoint path="BO1.PARSE_BOMESSAGE" connector-ref="VmConnector">
 			<vm:transaction action="ALWAYS_BEGIN" />
 		</vm:inbound-endpoint>

		<component>
			<method-entry-point-resolver>
				<include-entry-point method="parseAndPersistRawBOMessage" />
			</method-entry-point-resolver>
			<spring-object bean="boMessageProcessor" />
		</component>
		
		<vm:outbound-endpoint path="BO1.DoMatching" exchange-pattern="one-way" connector-ref="VmConnector" >
			<vm:transaction action="NONE" />
		</vm:outbound-endpoint>

		<!-- explicit definition of rollback max retry -->
		<rollback-exception-strategy maxRedeliveryAttempts="1">
      		<on-redelivery-attempts-exceeded>
	        	<!-- TODO: As of now, we have not set action.We can either redirect to a Q:
	           	<jms:outbound-endpoint queue="BO1.PARSE_BOMESSAGE.DLQ"> 
	            	<jms:transaction action="ALWAYS_JOIN"/>  
	         	</jms:outbound-endpoint> 
	         
	         	OR ::::: log error in a file/DB (JDBC connector)
	         	<file:outbound-endpoint path="exceptions" outputPattern="Exception-[function:uuid].xml"/>
	         	-->     
         	</on-redelivery-attempts-exceeded> 
   		</rollback-exception-strategy>
	</flow>
	
	<!-- Transaction 3: 
	      1. Invoke matching stored procedure, 
	      2. Update matching results to BO and Channel data table,
	      3. Insert in history tables
	      4. Chunk 
          5. Persist chunks to chunks data table
	 -->    
	<flow name="BO1.OfacMatchingAndMessageTransformationFlow">
		
		<vm:inbound-endpoint path="BO1.DoMatching" connector-ref="VmConnector">
 			<vm:transaction action="ALWAYS_BEGIN" />
 		</vm:inbound-endpoint>
		
		<component>
			<method-entry-point-resolver>
				<include-entry-point method="processParsedBOMessage" />
			</method-entry-point-resolver>
			<spring-object bean="boMessageProcessor" />
		</component>

		<component>
			<method-entry-point-resolver>
				<include-entry-point method="format" />
			</method-entry-point-resolver>
			<spring-object bean="fsMessageFormatter" />
		</component>
		
		<component>
			<method-entry-point-resolver>
				<include-entry-point method="chunkMessage" />
			</method-entry-point-resolver>
			<spring-object bean="fsMessageChunker" />
		</component>
		
		<vm:outbound-endpoint path="BO1.SendFsMessageFlow" exchange-pattern="one-way" connector-ref="VmConnector" >
			<vm:transaction action="NONE" />
		</vm:outbound-endpoint>

		<!-- explicit defn of rollback max retry -->
		 <rollback-exception-strategy maxRedeliveryAttempts="1">
      		<on-redelivery-attempts-exceeded>
        <!-- TODO: As of now, we have not set action. We can either redirect to a Q:
           		<jms:outbound-endpoint queue="BO1.OfacMatchingAndMessageTransformationFlow.DLQ"> 
            		<jms:transaction action="ALWAYS_JOIN"/>  
         		</jms:outbound-endpoint> 
         
         	OR ::::: log error in a file/DB (JDBC connector)
         		<file:outbound-endpoint path="exceptions" outputPattern="Exception-[function:uuid].xml"/>
         -->     
         	</on-redelivery-attempts-exceeded> 
   		</rollback-exception-strategy>
	</flow>

	
	<!-- Transaction 4 : Send messages to Frcosoft
          1. Send to fircosoft for sanction screening
          2. Update data life status(4) in BO and Channel data tables
	 -->  
	<flow name="BO1.OfacSenderFlow">
		
		<vm:inbound-endpoint path="BO1.SendFsMessageFlow" connector-ref="VmConnector">
 			<vm:transaction action="ALWAYS_BEGIN" />
 		</vm:inbound-endpoint>
			
		<component>
			<spring-object bean="fsMessageSender" />
		</component>

		<!-- explicit defn of rollback max retry -->
		 <rollback-exception-strategy maxRedeliveryAttempts="1">
      		<on-redelivery-attempts-exceeded>
        		<!-- TODO: As of now, we have not set action. We can either redirect to a Q:
           		<jms:outbound-endpoint queue="BO1.SendFsMessageFlow.DLQ"> 
            		<jms:transaction action="ALWAYS_JOIN"/>  
         		</jms:outbound-endpoint> 
         		
         		OR ::::: log error in a file/DB (JDBC connector)
         		<file:outbound-endpoint path="exceptions" outputPattern="Exception-[function:uuid].xml"/>
         		-->     
         	</on-redelivery-attempts-exceeded> 
   		</rollback-exception-strategy>
	</flow>
	
	<!-- Transaction 5. Receive OFAC Response -->
	<flow name="BO1.OfacAckQueueResponseFlow">
		
		<composite-source>
			<!-- transaction starts here -->
			<jms:inbound-endpoint ref="OFAC1.ACK.IN.QUEUE" >
				<jms:transaction action="ALWAYS_BEGIN" />
 			</jms:inbound-endpoint>
		</composite-source>

		<component>
			<method-entry-point-resolver>
				<include-entry-point method="processOFACResponseOnAckQueue" />
			</method-entry-point-resolver>
			<spring-object bean="ofacMessageProcessor" />
		</component>

		<vm:outbound-endpoint path="OFAC.OFACResponse" exchange-pattern="one-way" connector-ref="VmConnector" >
			<vm:transaction action="NONE" />
		</vm:outbound-endpoint>
		
		<!-- explicit defn of rollback max retry -->
		<rollback-exception-strategy maxRedeliveryAttempts="1">
			<on-redelivery-attempts-exceeded>
        	
        	<!-- TODO: As of now, we have not set action. We can either redirect to a Q:
           	<jms:outbound-endpoint queue="BO1.OfacResponseFlow.DLQ"> 
            	<jms:transaction action="ALWAYS_JOIN"/>  
         	</jms:outbound-endpoint> 
         	OR ::::: log error in a file/DB (JDBC connector)
         
         	<file:outbound-endpoint path="exceptions" outputPattern="Exception-[function:uuid].xml"/>
         	-->     
         	</on-redelivery-attempts-exceeded> 
      	</rollback-exception-strategy>
		
	</flow>

	<flow name="BO1.OfacOutQueueResponseFlow">
		
		<composite-source>
			<!-- transaction starts here -->			
			<jms:inbound-endpoint ref="OFAC1.DECISION.IN.QUEUE" >
				<jms:transaction action="ALWAYS_BEGIN" />
 			</jms:inbound-endpoint>
		</composite-source>
		
		<component>
			<spring-object bean="ofacMessageOutQueueProcessor" />
		</component>
		
		<vm:outbound-endpoint path="OFAC.OFACResponse" exchange-pattern="one-way" connector-ref="VmConnector" >
			<vm:transaction action="NONE" />
		</vm:outbound-endpoint>
		
		<!-- explicit defn of rollback max retry -->
		<rollback-exception-strategy maxRedeliveryAttempts="1">
			<on-redelivery-attempts-exceeded>
        	
        	<!-- TODO: As of now, we have not set action. We can either redirect to a Q:
           	<jms:outbound-endpoint queue="BO1.OfacResponseFlow.DLQ"> 
            	<jms:transaction action="ALWAYS_JOIN"/>  
         	</jms:outbound-endpoint> 
         	OR ::::: log error in a file/DB (JDBC connector)
         
         	<file:outbound-endpoint path="exceptions" outputPattern="Exception-[function:uuid].xml"/>
         	-->     
         	</on-redelivery-attempts-exceeded> 
      	</rollback-exception-strategy>
		
	</flow>
	
	<!-- Transaction 6. Send OFAC response to Back Office -->
	<flow name="BO1.BOSenderFlow">
		<vm:inbound-endpoint path="OFAC.OFACResponse" connector-ref="VmConnector" >
		<!-- transaction starts here -->
 			<vm:transaction action="NONE" />
 		</vm:inbound-endpoint>
		
		<component>
			<spring-object bean="boMessageSender" />
		</component>
		
		<!-- explicit defn of rollback max retry -->
		 <rollback-exception-strategy maxRedeliveryAttempts="1">
      		<on-redelivery-attempts-exceeded>
	        	<!-- TODO: As of now, we have not set action. We can either redirect to a Q:
	           	<jms:outbound-endpoint queue="BO1.BOSenderFlow.DLQ"> 
	            	<jms:transaction action="ALWAYS_JOIN"/>  
	         	</jms:outbound-endpoint> 
	         	
	         	OR ::::: log error in a file/DB (JDBC connector)
	         	<file:outbound-endpoint path="exceptions" outputPattern="Exception-[function:uuid].xml"/>
	         	-->     
         	</on-redelivery-attempts-exceeded> 
   		</rollback-exception-strategy>
	</flow>

</mule>','mule',
    CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'SEC_SYS',CURRENT_TIMESTAMP);
