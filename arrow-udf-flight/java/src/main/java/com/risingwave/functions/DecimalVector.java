// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.risingwave.functions;

import org.apache.arrow.vector.types.pojo.ArrowType.ExtensionType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.math.BigDecimal;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.vector.ExtensionTypeVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;

class DecimalType extends ExtensionType {

    @Override
    public ArrowType storageType() {
        return new ArrowType.Utf8();
    }

    @Override
    public String extensionName() {
        return "arrowudf.decimal";
    }

    @Override
    public boolean extensionEquals(ExtensionType other) {
        return other instanceof DecimalType;
    }

    @Override
    public ArrowType deserialize(ArrowType storageType, String serializedData) {
        if (!storageType.equals(storageType())) {
            throw new UnsupportedOperationException("Cannot construct DecimalType from underlying type " + storageType);
        }
        return new DecimalType();
    }

    @Override
    public String serialize() {
        return "";
    }

    @Override
    public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator) {
        return new DecimalVector(name, allocator);
    }
}

public class DecimalVector extends ExtensionTypeVector<VarCharVector> {

    public DecimalVector(String name, BufferAllocator allocator) {
        super(name, allocator, new VarCharVector(name, allocator));
    }

    public void allocateNew(int valueCount) {
        getUnderlyingVector().allocateNew(valueCount);
    }

    @Override
    public BigDecimal getObject(int index) {
        var text = getUnderlyingVector().getObject(index);
        if (text == null)
            return null;
        return new BigDecimal(text.toString());
    }

    public void set(int index, BigDecimal decimal) {
        // use `toPlainString` to avoid scientific notation
        getUnderlyingVector().set(index, decimal.toPlainString().getBytes());
    }

    @Override
    public int hashCode(int index) {
        return hashCode(index, null);
    }

    @Override
    public int hashCode(int index, ArrowBufHasher hasher) {
        return getUnderlyingVector().hashCode(index, hasher);
    }

    @Override
    public Field getField() {
        return new Field(getName(), FieldType.nullable(new DecimalType()), null);
    }
}
