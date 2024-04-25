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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class UdfProducer extends NoOpFlightProducer {

    private BufferAllocator allocator;
    private HashMap<String, UserDefinedFunctionBatch> functions = new HashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(UdfServer.class);

    UdfProducer(BufferAllocator allocator) {
        this.allocator = allocator;
    }

    void addFunction(String name, UserDefinedFunction function) throws IllegalArgumentException {
        UserDefinedFunctionBatch udf;
        if (function instanceof ScalarFunction) {
            udf = new ScalarFunctionBatch((ScalarFunction) function);
        } else if (function instanceof TableFunction) {
            udf = new TableFunctionBatch((TableFunction) function);
        } else {
            throw new IllegalArgumentException(
                    "Unknown function type: " + function.getClass().getName());
        }
        if (functions.containsKey(name)) {
            throw new IllegalArgumentException("Function already exists: " + name);
        }
        functions.put(name, udf);
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        try {
            var functionName = descriptor.getPath().get(0);
            var udf = functions.get(functionName);
            if (udf == null) {
                throw new IllegalArgumentException("Unknown function: " + functionName);
            }
            var fields = new ArrayList<Field>();
            fields.addAll(udf.getInputSchema().getFields());
            fields.addAll(udf.getOutputSchema().getFields());
            var fullSchema = new Schema(fields);
            var inputLen = udf.getInputSchema().getFields().size();

            return new FlightInfo(fullSchema, descriptor, Collections.emptyList(), 0, inputLen);
        } catch (Exception e) {
            logger.error("Error occurred during getFlightInfo", e);
            throw e;
        }
    }

    @Override
    public void doExchange(CallContext context, FlightStream reader, ServerStreamListener writer) {
        try (var allocator = this.allocator.newChildAllocator("exchange", 0, Long.MAX_VALUE)) {
            var functionName = reader.getDescriptor().getPath().get(0);
            logger.debug("call function: " + functionName);

            var udf = this.functions.get(functionName);
            try (var root = VectorSchemaRoot.create(udf.getOutputSchema(), allocator)) {
                var loader = new VectorLoader(root);
                writer.start(root);
                while (reader.next()) {
                    try (var input = reader.getRoot()) {
                        var outputBatches = udf.evalBatch(input, allocator);
                        while (outputBatches.hasNext()) {
                            try (var outputRoot = outputBatches.next()) {
                                var unloader = new VectorUnloader(outputRoot);
                                try (var outputBatch = unloader.getRecordBatch()) {
                                    loader.load(outputBatch);
                                }
                            }
                            writer.putNext();
                        }
                    }
                }
                writer.completed();
            }
        } catch (Exception e) {
            logger.error("Error occurred during UDF execution", e);
            writer.error(e);
        }
    }
}
