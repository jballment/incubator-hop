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

/** Optional model settings shared by native chat providers. Null means use the provider default. */
public record AiChatOptions(
    Double temperature,
    Double topP,
    Integer topK,
    Integer contextSize,
    Integer maxOutputTokens,
    Boolean thinking,
    Integer seed,
    Double presencePenalty,
    Double frequencyPenalty,
    Integer maxRetries,
    boolean structuredOutput,
    AiChatProxy proxy) {

  public static AiChatOptions defaults() {
    return new AiChatOptions(
        null, null, null, null, null, null, null, null, null, null, false, null);
  }

  public AiChatOptions(
      Double temperature,
      Double topP,
      Integer topK,
      Integer contextSize,
      Integer maxOutputTokens,
      Boolean thinking,
      Integer seed,
      Double presencePenalty,
      Double frequencyPenalty,
      Integer maxRetries,
      boolean structuredOutput) {
    this(
        temperature,
        topP,
        topK,
        contextSize,
        maxOutputTokens,
        thinking,
        seed,
        presencePenalty,
        frequencyPenalty,
        maxRetries,
        structuredOutput,
        null);
  }
}
