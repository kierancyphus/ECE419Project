package testing;

import org.junit.Test;

import junit.framework.TestCase;

public class AdditionalTest extends TestCase {
	
	// TODO add your test cases, at least 3
	
	@Test
	public void testStub() {
		assertTrue(true);
	}


	@Test
	public void testString() {
		String test = "something" + null + " else";
		System.out.println(test);
	}

}
