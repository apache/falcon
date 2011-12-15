package org.apache.airavat.converter;

import junit.framework.Assert;

import org.apache.airavat.AiravatException;
import org.apache.airavat.entity.parser.EntityParserFactory;
import org.apache.airavat.entity.parser.ProcessEntityParser;
import org.apache.airavat.entity.v0.EntityType;
import org.apache.airavat.entity.v0.ProcessType;
import org.apache.airavat.oozie.coordinator.COORDINATORAPP;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class CoordinatorConverterTest {

	private ProcessType processType;
	private static final String SAMPLE_PROCESS_XML = "/process-version-0.xml";

	@BeforeClass
	public void populateProcessType() throws AiravatException {
		ProcessEntityParser parser = (ProcessEntityParser) EntityParserFactory
				.getParser(EntityType.PROCESS);

		processType = (ProcessType) parser.parse(this.getClass()
				.getResourceAsStream(SAMPLE_PROCESS_XML));

	}

	@Test
	public void testConvert() {
		COORDINATORAPP coordinatorapp = CoordinatorConverter.convert(
				processType, null, null);
		Assert.assertNotNull(coordinatorapp);
	}
}
