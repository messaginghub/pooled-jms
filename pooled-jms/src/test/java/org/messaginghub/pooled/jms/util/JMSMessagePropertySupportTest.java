/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.messaginghub.pooled.jms.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import java.util.Collections;

import javax.jms.JMSException;
import javax.jms.MessageFormatException;

import org.junit.Test;
import org.messaginghub.pooled.jms.util.JMSMessagePropertySupport;

/**
 * Test for the JMS Message Property support class
 */
public class JMSMessagePropertySupportTest {

    //----- convertPropertyTo tests ------------------------------------------//

    @Test
    public void testConvertPropertyToNullBooleanTarget() throws JMSException {
        assertFalse(JMSMessagePropertySupport.convertPropertyTo("timeout", null, Boolean.class));
    }

    @Test(expected = NullPointerException.class)
    public void testConvertPropertyToNullFloatTarget() throws JMSException {
        JMSMessagePropertySupport.convertPropertyTo("float", null, Float.class);
    }

    @Test(expected = NullPointerException.class)
    public void testConvertPropertyToNullDoubleTarget() throws JMSException {
        JMSMessagePropertySupport.convertPropertyTo("double", null, Double.class);
    }

    @Test(expected = NumberFormatException.class)
    public void testConvertPropertyToNullShortTarget() throws JMSException {
        JMSMessagePropertySupport.convertPropertyTo("number", null, Short.class);
    }

    @Test(expected = NumberFormatException.class)
    public void testConvertPropertyToNullIntegerTarget() throws JMSException {
        JMSMessagePropertySupport.convertPropertyTo("number", null, Integer.class);
    }

    @Test(expected = NumberFormatException.class)
    public void testConvertPropertyToNullLongTarget() throws JMSException {
        JMSMessagePropertySupport.convertPropertyTo("number", null, Long.class);
    }

    @Test
    public void testConvertPropertyToNullStringTarget() throws JMSException {
        assertNull(JMSMessagePropertySupport.convertPropertyTo("string", null, String.class));
    }

    //----- checkPropertyNameIsValid tests -----------------------------------//

    @Test(expected = IllegalArgumentException.class)
    public void testCheckPropertyNameIsValidWithNullName() throws JMSException {
        JMSMessagePropertySupport.checkPropertyNameIsValid(null, true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckPropertyNameIsValidWithEmptyName() throws JMSException {
        JMSMessagePropertySupport.checkPropertyNameIsValid("", true);
    }

    @Test
    public void testCheckPropertyNameWithLogicOperatorValidationDisabled() throws JMSException {
        JMSMessagePropertySupport.checkPropertyNameIsValid("OR", false);
    }

    //----- checkIdentifierIsntLogicOperator tests ---------------------------//

    @Test(expected = IllegalArgumentException.class)
    public void testCheckIdentifierIsntLogicOperatorOr() throws JMSException {
        JMSMessagePropertySupport.checkIdentifierIsntLogicOperator("OR");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckIdentifierIsntLogicOperatorAnd() throws JMSException {
        JMSMessagePropertySupport.checkIdentifierIsntLogicOperator("AND");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckIdentifierIsntLogicOperatorNot() throws JMSException {
        JMSMessagePropertySupport.checkIdentifierIsntLogicOperator("NOT");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckIdentifierIsntLogicOperatorBetween() throws JMSException {
        JMSMessagePropertySupport.checkIdentifierIsntLogicOperator("BETWEEN");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckIdentifierIsntLogicOperatorIn() throws JMSException {
        JMSMessagePropertySupport.checkIdentifierIsntLogicOperator("IN");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckIdentifierIsntLogicOperatorLike() throws JMSException {
        JMSMessagePropertySupport.checkIdentifierIsntLogicOperator("LIKE");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckIdentifierIsntLogicOperatorIs() throws JMSException {
        JMSMessagePropertySupport.checkIdentifierIsntLogicOperator("IS");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckIdentifierIsntLogicOperatorEscape() throws JMSException {
        JMSMessagePropertySupport.checkIdentifierIsntLogicOperator("ESCAPE");
    }

    //----- checkIdentifierIsntNullTrueFalse tests ---------------------------//

    @Test(expected = IllegalArgumentException.class)
    public void testCheckIdentifierIsntNullTrueFalseFalse() throws JMSException {
        JMSMessagePropertySupport.checkIdentifierIsntNullTrueFalse("FALSE");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckIdentifierIsntNullTrueFalseNull() throws JMSException {
        JMSMessagePropertySupport.checkIdentifierIsntNullTrueFalse("NULL");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckIdentifierIsntNullTrueFalseTrue() throws JMSException {
        JMSMessagePropertySupport.checkIdentifierIsntNullTrueFalse("TRUE");
    }

    //----- checkIdentifierLetterAndDigitRequirements ------------------------//

    @Test
    public void testCheckIdentifierLetterAndDigitRequirementsValid() throws JMSException {
        JMSMessagePropertySupport.checkIdentifierLetterAndDigitRequirements("test");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckIdentifierLetterAndDigitRequirementsStartWithNumber() throws JMSException {
        JMSMessagePropertySupport.checkIdentifierLetterAndDigitRequirements("1");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckIdentifierLetterAndDigitRequirementsContainsColon() throws JMSException {
        JMSMessagePropertySupport.checkIdentifierLetterAndDigitRequirements("a:b");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckIdentifierLetterAndDigitRequirementsContainsEndWithColon() throws JMSException {
        JMSMessagePropertySupport.checkIdentifierLetterAndDigitRequirements("a:b");
    }

    //----- checkValidObject -------------------------------------------------//

    @Test
    public void testCheckValidObjectBoolean() throws JMSException {
        JMSMessagePropertySupport.checkValidObject(Boolean.TRUE);
    }

    @Test
    public void testCheckValidObjectByte() throws JMSException {
        JMSMessagePropertySupport.checkValidObject(Byte.MAX_VALUE);
    }

    @Test
    public void testCheckValidObjectShort() throws JMSException {
        JMSMessagePropertySupport.checkValidObject(Short.MAX_VALUE);
    }

    @Test
    public void testCheckValidObjectInteger() throws JMSException {
        JMSMessagePropertySupport.checkValidObject(Integer.MAX_VALUE);
    }

    @Test
    public void testCheckValidObjectLong() throws JMSException {
        JMSMessagePropertySupport.checkValidObject(Long.MAX_VALUE);
    }

    @Test
    public void testCheckValidObjectFloat() throws JMSException {
        JMSMessagePropertySupport.checkValidObject(Float.MAX_VALUE);
    }

    @Test
    public void testCheckValidObjectDouble() throws JMSException {
        JMSMessagePropertySupport.checkValidObject(Double.MAX_VALUE);
    }

    @Test
    public void testCheckValidObjectCharacter() throws JMSException {
        JMSMessagePropertySupport.checkValidObject(Character.MAX_VALUE);
    }

    @Test
    public void testCheckValidObjectString() throws JMSException {
        JMSMessagePropertySupport.checkValidObject("");
    }

    @Test
    public void testCheckValidObjectNull() throws JMSException {
        JMSMessagePropertySupport.checkValidObject(null);
    }

    @Test(expected = MessageFormatException.class)
    public void testCheckValidObjectList() throws JMSException {
        JMSMessagePropertySupport.checkValidObject(Collections.EMPTY_LIST);
    }
}
