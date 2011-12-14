package org.apache.airavat;

import java.io.InputStream;

import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.validation.Schema;

import org.testng.Assert;
import org.testng.annotations.Test;
import org.xml.sax.SAXException;

public class UtilTest {

	@Test
	public void getSchema() throws SAXException {
		Schema schema = Util.getSchema(UtilTest.class
				.getResource("/coordinator.xsd"));
		Assert.assertNotNull(schema);
	}

	@Test
	public void getStreamFromString() {
		InputStream stream = Util.getStreamFromString("<test>hi</test>");
		Assert.assertNotNull(stream);
	}

	@Test
	public void getUnmarshaller() throws JAXBException {
		Unmarshaller unmarshaller = Util.getUnmarshaller(UtilTest.class);
		Assert.assertNotNull(unmarshaller);
	}

	@Test
	public void getMarshaller() throws JAXBException {
		Marshaller marshaller = Util.getMarshaller(UtilTest.class);
		Assert.assertNotNull(marshaller);
	}
}
