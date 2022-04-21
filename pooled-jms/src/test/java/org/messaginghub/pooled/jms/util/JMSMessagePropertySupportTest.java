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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;

import jakarta.jms.JMSException;
import jakarta.jms.MessageFormatException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Test for the JMS Message Property support class
 */
@Timeout(20)
public class JMSMessagePropertySupportTest {

    //----- convertPropertyTo tests ------------------------------------------//

    @Test
    public void testConvertPropertyToNullBooleanTarget() throws JMSException {
        assertFalse(JMSMessagePropertySupport.convertPropertyTo("timeout", null, Boolean.class));
    }

    @Test
    public void testConvertPropertyToNullFloatTarget() throws JMSException {
        assertThrows(NullPointerException.class, () -> {
            JMSMessagePropertySupport.convertPropertyTo("float", null, Float.class);
        });
    }

    @Test
    public void testConvertPropertyToNullDoubleTarget() throws JMSException {
        assertThrows(NullPointerException.class, () -> {
            JMSMessagePropertySupport.convertPropertyTo("double", null, Double.class);
        });
    }

    @Test
    public void testConvertPropertyToNullShortTarget() throws JMSException {
        assertThrows(NumberFormatException.class, () -> {
            JMSMessagePropertySupport.convertPropertyTo("number", null, Short.class);
        });
    }

    @Test
    public void testConvertPropertyToNullIntegerTarget() throws JMSException {
        assertThrows(NumberFormatException.class, () -> {
            JMSMessagePropertySupport.convertPropertyTo("number", null, Integer.class);
        });
    }

    @Test
    public void testConvertPropertyToNullLongTarget() throws JMSException {
        assertThrows(NumberFormatException.class, () -> {
            JMSMessagePropertySupport.convertPropertyTo("number", null, Long.class);
        });
    }

    @Test
    public void testConvertPropertyToNullStringTarget() throws JMSException {
        assertNull(JMSMessagePropertySupport.convertPropertyTo("string", null, String.class));
    }

    //----- checkPropertyNameIsValid tests -----------------------------------//

    @Test
    public void testCheckPropertyNameIsValidWithNullName() throws JMSException {
        assertThrows(IllegalArgumentException.class, () -> {
            JMSMessagePropertySupport.checkPropertyNameIsValid(null, true);
        });
    }

    @Test
    public void testCheckPropertyNameIsValidWithEmptyName() throws JMSException {
        assertThrows(IllegalArgumentException.class, () -> {
            JMSMessagePropertySupport.checkPropertyNameIsValid("", true);
        });
    }

    @Test
    public void testCheckPropertyNameWithLogicOperatorValidationDisabled() throws JMSException {
        JMSMessagePropertySupport.checkPropertyNameIsValid("OR", false);
    }

    //----- checkIdentifierIsntLogicOperator tests ---------------------------//

    @Test
    public void testCheckIdentifierIsntLogicOperatorOr() throws JMSException {
        assertThrows(IllegalArgumentException.class, () -> {
            JMSMessagePropertySupport.checkIdentifierIsntLogicOperator("OR");
        });
    }

    @Test
    public void testCheckIdentifierIsntLogicOperatorAnd() throws JMSException {
        assertThrows(IllegalArgumentException.class, () -> {
            JMSMessagePropertySupport.checkIdentifierIsntLogicOperator("AND");
        });
    }

    @Test
    public void testCheckIdentifierIsntLogicOperatorNot() throws JMSException {
        assertThrows(IllegalArgumentException.class, () -> {
            JMSMessagePropertySupport.checkIdentifierIsntLogicOperator("NOT");
        });
    }

    @Test
    public void testCheckIdentifierIsntLogicOperatorBetween() throws JMSException {
        assertThrows(IllegalArgumentException.class, () -> {
            JMSMessagePropertySupport.checkIdentifierIsntLogicOperator("BETWEEN");
        });
    }

    @Test
    public void testCheckIdentifierIsntLogicOperatorIn() throws JMSException {
        assertThrows(IllegalArgumentException.class, () -> {
            JMSMessagePropertySupport.checkIdentifierIsntLogicOperator("IN");
        });
    }

    @Test
    public void testCheckIdentifierIsntLogicOperatorLike() throws JMSException {
        assertThrows(IllegalArgumentException.class, () -> {
            JMSMessagePropertySupport.checkIdentifierIsntLogicOperator("LIKE");
        });
    }

    @Test
    public void testCheckIdentifierIsntLogicOperatorIs() throws JMSException {
        assertThrows(IllegalArgumentException.class, () -> {
            JMSMessagePropertySupport.checkIdentifierIsntLogicOperator("IS");
        });
    }

    @Test
    public void testCheckIdentifierIsntLogicOperatorEscape() throws JMSException {
        assertThrows(IllegalArgumentException.class, () -> {
            JMSMessagePropertySupport.checkIdentifierIsntLogicOperator("ESCAPE");
        });
    }

    //----- checkIdentifierIsntNullTrueFalse tests ---------------------------//

    @Test
    public void testCheckIdentifierIsntNullTrueFalseFalse() throws JMSException {
        assertThrows(IllegalArgumentException.class, () -> {
            JMSMessagePropertySupport.checkIdentifierIsntNullTrueFalse("FALSE");
        });
    }

    @Test
    public void testCheckIdentifierIsntNullTrueFalseNull() throws JMSException {
        assertThrows(IllegalArgumentException.class, () -> {
            JMSMessagePropertySupport.checkIdentifierIsntNullTrueFalse("NULL");
        });
    }

    @Test
    public void testCheckIdentifierIsntNullTrueFalseTrue() throws JMSException {
        assertThrows(IllegalArgumentException.class, () -> {
            JMSMessagePropertySupport.checkIdentifierIsntNullTrueFalse("TRUE");
        });
    }

    //----- checkIdentifierLetterAndDigitRequirements ------------------------//

    @Test
    public void testCheckIdentifierLetterAndDigitRequirementsValid() throws JMSException {
        JMSMessagePropertySupport.checkIdentifierLetterAndDigitRequirements("test");
    }

    @Test
    public void testCheckIdentifierLetterAndDigitRequirementsStartWithNumber() throws JMSException {
        assertThrows(IllegalArgumentException.class, () -> {
            JMSMessagePropertySupport.checkIdentifierLetterAndDigitRequirements("1");
        });
    }

    @Test
    public void testCheckIdentifierLetterAndDigitRequirementsContainsColon() throws JMSException {
        assertThrows(IllegalArgumentException.class, () -> {
            JMSMessagePropertySupport.checkIdentifierLetterAndDigitRequirements("a:b");
        });
    }

    @Test
    public void testCheckIdentifierLetterAndDigitRequirementsContainsEndWithColon() throws JMSException {
        assertThrows(IllegalArgumentException.class, () -> {
            JMSMessagePropertySupport.checkIdentifierLetterAndDigitRequirements("a:b");
        });
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

    @Test
    public void testCheckValidObjectList() throws JMSException {
        assertThrows(MessageFormatException.class, () -> {
            JMSMessagePropertySupport.checkValidObject(Collections.EMPTY_LIST);
        });
    }
}
