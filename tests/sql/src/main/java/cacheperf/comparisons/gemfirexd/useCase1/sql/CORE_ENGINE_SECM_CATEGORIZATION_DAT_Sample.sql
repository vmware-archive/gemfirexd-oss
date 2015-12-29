SET CURRENT SCHEMA=SEC_OWNER;

INSERT
INTO SECM_CATEGORIZATION
  (
    CATEG_ID,
    CATEG_TYPE,
    CATEG_DESC,
	ONBOARD_DATE,
	END_DATE,
    LAST_UPDATE_DATE,
    LAST_UPDATE_BY
  )
  VALUES
  (
    1,
    'Expected Unmatched',
    'Any Back Office transaction which has been deliberately excluded from the Securitas matching process, where it is known that the corresponding Channel Input system has not yet been onboarded to Securitas.',
    CURRENT TIMESTAMP,
	TIMESTAMP('2020-03-19', '00:00:00'),
	CURRENT TIMESTAMP,
    'SEC_SYS'
  );

INSERT
INTO SECM_CATEGORIZATION
  (
    CATEG_ID,
    CATEG_TYPE,
    CATEG_DESC,
	ONBOARD_DATE,
	END_DATE,
    LAST_UPDATE_DATE,
    LAST_UPDATE_BY
  )
  VALUES
  (
    2,
    'Unexpected Unmatched',
    'Any Back office transaction that cannot be matched to a Channel input:
             1. Channel data transmission error delay
             2. Transaction created within back office
             3. Unrecognized new file format or data type
             4. Other errors
    ',
    CURRENT TIMESTAMP,
	TIMESTAMP('2020-03-19', '00:00:00'),
	CURRENT TIMESTAMP,
    'SEC_SYS'
  );
  
INSERT
INTO SECM_CATEGORIZATION
  (
    CATEG_ID,
    CATEG_TYPE,
    CATEG_DESC,
	ONBOARD_DATE,
	END_DATE,
    LAST_UPDATE_DATE,
    LAST_UPDATE_BY
  )
  VALUES
  (
    3,
    'Ineligible Unmatched',
    'Any Back Office transaction which has been deliberately excluded from the Securitas matching/scanning process. Eg ATRs',
    CURRENT TIMESTAMP,
	TIMESTAMP('2020-03-19', '00:00:00'),
	CURRENT TIMESTAMP,
    'SEC_SYS'
  );

  INSERT
INTO SECM_CATEGORIZATION
  (
    CATEG_ID,
    CATEG_TYPE,
    CATEG_DESC,
	ONBOARD_DATE,
	END_DATE,
    LAST_UPDATE_DATE,
    LAST_UPDATE_BY
  )
  VALUES
  (
    4,
    'Multi-matched',
    'Multi-matched due to
             1. Unrecognized new file format or data type
             2. Duplicate channel data sent to Securitas
             3. Client transaction ref number recycled
             4. Channel transaction missing matching key(s)
             5. Channel matching keys modified downstream
             6. Securitas matching logic error
    ',
    CURRENT TIMESTAMP,
	TIMESTAMP('2020-03-19', '00:00:00'),
	CURRENT TIMESTAMP,
    'SEC_SYS'
  );
  
INSERT
INTO SECM_CATEGORIZATION
  (
    CATEG_ID,
    CATEG_TYPE,
    CATEG_DESC,
	ONBOARD_DATE,
	END_DATE,
    LAST_UPDATE_DATE,
    LAST_UPDATE_BY
  )
  VALUES
  (
    5,
    'Matched',
    'A Back Office transaction or Input Channel instruction that has been correctly matched.',
    CURRENT TIMESTAMP,
	TIMESTAMP('2020-03-19', '00:00:00'),
	CURRENT TIMESTAMP,
    'SEC_SYS'
  );

INSERT
INTO SECM_CATEGORIZATION
  (
    CATEG_ID,
    CATEG_TYPE,
    CATEG_DESC,
	ONBOARD_DATE,
	END_DATE,
    LAST_UPDATE_DATE,
    LAST_UPDATE_BY
  )
  VALUES
  (
    6,
    'Expected Orphan',
    'Any Input Channel instruction which has been deliberately excluded from the Securitas matching process, where it is known that the corresponding Back Office system has not yet been onboarded to Securitas.',
    CURRENT TIMESTAMP,
	TIMESTAMP('2020-03-19', '00:00:00'),
	CURRENT TIMESTAMP,
    'SEC_SYS'
  );

INSERT
INTO SECM_CATEGORIZATION
  (
    CATEG_ID,
    CATEG_TYPE,
    CATEG_DESC,
	ONBOARD_DATE,
	END_DATE,
    LAST_UPDATE_DATE,
    LAST_UPDATE_BY
  )
  VALUES
  (
    7,
    'Unexpected Orphan',
    'Any Input Channel instruction which cannot be matched to a Back Office transaction after a given period of time. Eg Where Back Office transaction has been cancelled without notification to Securitas.',
    CURRENT TIMESTAMP,
	TIMESTAMP('2020-03-19', '00:00:00'),
	CURRENT TIMESTAMP,
    'SEC_SYS'
  );
