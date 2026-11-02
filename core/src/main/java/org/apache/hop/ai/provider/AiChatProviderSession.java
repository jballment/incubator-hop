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

package org.apache.hop.ai.provider;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;

/** A named, resolved native chat backend and its connection-level settings. */
public record AiChatProviderSession(
    IAiChatProvider provider,
    String providerName,
    String modelType,
    String modelName,
    Duration timeout,
    AiChatOptions options) {

  public AiChatProviderSession {
    options = options == null ? AiChatOptions.defaults() : options;
  }

  public AiChatResponse chat(AiChatRequest request, IVariables variables) throws HopException {
    AiChatOptions requestOptions = request.options();
    AiChatOptions effectiveOptions =
        new AiChatOptions(
            choose(options.temperature(), requestOptions.temperature()),
            choose(options.topP(), requestOptions.topP()),
            choose(options.topK(), requestOptions.topK()),
            choose(options.contextSize(), requestOptions.contextSize()),
            choose(options.maxOutputTokens(), requestOptions.maxOutputTokens()),
            choose(options.thinking(), requestOptions.thinking()),
            choose(options.seed(), requestOptions.seed()),
            choose(options.presencePenalty(), requestOptions.presencePenalty()),
            choose(options.frequencyPenalty(), requestOptions.frequencyPenalty()),
            choose(options.maxRetries(), requestOptions.maxRetries()),
            options.structuredOutput() || requestOptions.structuredOutput(),
            choose(options.proxy(), requestOptions.proxy()));
    AiChatRequest configured =
        new AiChatRequest(
            nonEmpty(modelName) ? modelName : request.modelName(),
            request.systemPrompt(),
            request.userPrompt(),
            request.conversationHistory(),
            timeout != null ? timeout : request.timeout(),
            request.jsonOnly(),
            effectiveOptions);
    List<String> unsupported = unsupportedOptions(provider.capabilities(), configured);
    if (!unsupported.isEmpty()) {
      throw new HopException(
          "AI provider "
              + providerName
              + " does not support Language Model Chat options: "
              + String.join(", ", unsupported));
    }
    return provider.chat(configured, variables);
  }

  private static List<String> unsupportedOptions(
      Set<AiChatCapability> capabilities, AiChatRequest request) {
    List<String> unsupported = new ArrayList<>();
    AiChatOptions options = request.options();
    addIfUnsupported(
        unsupported, capabilities, AiChatCapability.MODEL_SELECTION, request.modelName(), "model");
    addIfUnsupported(
        unsupported, capabilities, AiChatCapability.REQUEST_TIMEOUT, request.timeout(), "timeout");
    addIfUnsupported(
        unsupported,
        capabilities,
        AiChatCapability.SYSTEM_PROMPT,
        request.systemPrompt(),
        "system prompt");
    addIfUnsupported(
        unsupported,
        capabilities,
        AiChatCapability.CONVERSATION_HISTORY,
        request.conversationHistory().isEmpty() ? null : request.conversationHistory(),
        "conversation history");
    addIfUnsupported(
        unsupported,
        capabilities,
        AiChatCapability.JSON_ONLY_PROMPT,
        request.jsonOnly() ? Boolean.TRUE : null,
        "JSON-only prompt");
    addIfUnsupported(
        unsupported,
        capabilities,
        AiChatCapability.TEMPERATURE,
        options.temperature(),
        "temperature");
    addIfUnsupported(unsupported, capabilities, AiChatCapability.TOP_P, options.topP(), "top P");
    addIfUnsupported(unsupported, capabilities, AiChatCapability.TOP_K, options.topK(), "top K");
    addIfUnsupported(
        unsupported,
        capabilities,
        AiChatCapability.CONTEXT_SIZE,
        options.contextSize(),
        "context size");
    addIfUnsupported(
        unsupported,
        capabilities,
        AiChatCapability.MAX_OUTPUT_TOKENS,
        options.maxOutputTokens(),
        "max output tokens");
    addIfUnsupported(
        unsupported, capabilities, AiChatCapability.THINKING, options.thinking(), "thinking");
    addIfUnsupported(
        unsupported,
        capabilities,
        AiChatCapability.STRUCTURED_OUTPUT,
        options.structuredOutput() ? Boolean.TRUE : null,
        "structured output");
    addIfUnsupported(unsupported, capabilities, AiChatCapability.SEED, options.seed(), "seed");
    addIfUnsupported(
        unsupported,
        capabilities,
        AiChatCapability.PRESENCE_PENALTY,
        options.presencePenalty(),
        "presence penalty");
    addIfUnsupported(
        unsupported,
        capabilities,
        AiChatCapability.FREQUENCY_PENALTY,
        options.frequencyPenalty(),
        "frequency penalty");
    addIfUnsupported(
        unsupported, capabilities, AiChatCapability.RETRIES, options.maxRetries(), "retries");
    addIfUnsupported(unsupported, capabilities, AiChatCapability.PROXY, options.proxy(), "proxy");
    return unsupported;
  }

  private static void addIfUnsupported(
      List<String> unsupported,
      Set<AiChatCapability> capabilities,
      AiChatCapability capability,
      Object value,
      String label) {
    if (value != null
        && (!(value instanceof String text) || !text.isBlank())
        && !capabilities.contains(capability)) {
      unsupported.add(label);
    }
  }

  private static boolean nonEmpty(String value) {
    return value != null && !value.isBlank();
  }

  private static <T> T choose(T preferred, T fallback) {
    return preferred != null ? preferred : fallback;
  }
}
