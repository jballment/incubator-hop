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

/** Capabilities a native chat provider can honor when used by a chat client or transform. */
public enum AiChatCapability {
  MODEL_SELECTION,
  REQUEST_TIMEOUT,
  SYSTEM_PROMPT,
  CONVERSATION_HISTORY,
  JSON_ONLY_PROMPT,
  TEMPERATURE,
  TOP_P,
  TOP_K,
  CONTEXT_SIZE,
  MAX_OUTPUT_TOKENS,
  THINKING,
  STRUCTURED_OUTPUT,
  SEED,
  PRESENCE_PENALTY,
  FREQUENCY_PENALTY,
  RETRIES,
  PROXY
}
