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
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.vector.ExtensionTypeVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;

class JsonType extends ExtensionType {

    @Override
    public ArrowType storageType() {
        return new ArrowType.Utf8();
    }

    @Override
    public String extensionName() {
        return "arrowudf.json";
    }

    @Override
    public boolean extensionEquals(ExtensionType other) {
        return other instanceof JsonType;
    }

    @Override
    public ArrowType deserialize(ArrowType storageType, String serializedData) {
        if (!storageType.equals(storageType())) {
            throw new UnsupportedOperationException("Cannot construct JsonType from underlying type " + storageType);
        }
        return new JsonType();
    }

    @Override
    public String serialize() {
        return "";
    }

    @Override
    public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator) {
        return new JsonVector(name, allocator);
    }
}

public class JsonVector extends ExtensionTypeVector<VarCharVector> {

    public JsonVector(String name, BufferAllocator allocator) {
        super(name, allocator, new VarCharVector(name, allocator));
    }

    public void allocateNew(int valueCount) {
        getUnderlyingVector().allocateNew(valueCount);
    }

    @Override
    public String getObject(int index) {
        var text = getUnderlyingVector().getObject(index);
        if (text == null)
            return null;
        return text.toString();
    }

    public void set(int index, String json) {
        getUnderlyingVector().set(index, json.getBytes());
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
        return new Field(getName(), FieldType.nullable(new JsonType()), null);
    }
}