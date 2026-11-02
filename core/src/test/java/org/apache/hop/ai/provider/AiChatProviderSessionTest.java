/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ai.provider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.Test;

class AiChatProviderSessionTest {

  @Test
  void validatesEffectiveConnectionOptionsBeforeCallingProvider() {
    TestProvider provider = new TestProvider(Set.of(AiChatCapability.REQUEST_TIMEOUT));
    AiChatProviderSession session =
        new AiChatProviderSession(
            provider,
            "test provider",
            "TEST",
            "test-model",
            Duration.ofSeconds(30),
            new AiChatOptions(0.2, null, null, null, null, null, null, null, null, null, false));

    HopException error =
        assertThrows(
            HopException.class,
            () ->
                session.chat(
                    new AiChatRequest(null, null, "hello", List.of(), null, false),
                    new Variables()));

    assertTrue(
        error
            .getMessage()
            .contains(
                "AI provider test provider does not support Language Model Chat options: "
                    + "model, temperature"));
    assertNull(provider.request);
  }

  @Test
  void connectionOptionsOverrideRequestOptionsAndBlankValuesAreIgnored() throws Exception {
    TestProvider provider =
        new TestProvider(
            EnumSet.of(
                AiChatCapability.MODEL_SELECTION,
                AiChatCapability.REQUEST_TIMEOUT,
                AiChatCapability.TEMPERATURE));
    AiChatProviderSession session =
        new AiChatProviderSession(
            provider,
            "test provider",
            "TEST",
            "configured-model",
            Duration.ofSeconds(30),
            new AiChatOptions(0.2, null, null, null, null, null, null, null, null, null, false));

    session.chat(
        new AiChatRequest(
            " ",
            null,
            "hello",
            List.of(),
            null,
            false,
            new AiChatOptions(0.8, null, null, null, null, null, null, null, null, null, false)),
        new Variables());

    assertEquals("configured-model", provider.request.modelName());
    assertEquals(Duration.ofSeconds(30), provider.request.timeout());
    assertEquals(Double.valueOf(0.2), provider.request.options().temperature());
  }

  private static final class TestProvider implements IAiChatProvider {
    private final Set<AiChatCapability> capabilities;
    private AiChatRequest request;

    private TestProvider(Set<AiChatCapability> capabilities) {
      this.capabilities = capabilities;
    }

    @Override
    public Set<AiChatCapability> capabilities() {
      return capabilities;
    }

    @Override
    public AiChatResponse chat(AiChatRequest request, IVariables variables) {
      this.request = request;
      return new AiChatResponse("ok", null, null, 0);
    }

    @Override
    public List<String> listModelNames(IVariables variables, Duration timeout) {
      return List.of("test-model");
    }
  }
}
