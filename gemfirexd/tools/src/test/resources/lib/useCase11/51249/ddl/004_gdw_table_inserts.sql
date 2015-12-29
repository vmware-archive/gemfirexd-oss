--Lookup added to check on the current quarter date for ECC booking filters  
--This Change the dates as per the Quarter.
INSERT INTO RTRADM.LOOKUP_D VALUES(999999,'NA','NA','RTR_QUARTER',999999,'RTR_QUARTER_CONTROL','2014-10-01','2014-12-31',CURRENT_DATE);

-- default value for cust site location
INSERT INTO RTRADM.CUST_SITE_LOCATION VALUES(2,-2,'NA','NA','A');