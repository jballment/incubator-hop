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

package org.apache.hop.pipeline.transforms.languagemodelchat;

import static java.lang.Runtime.getRuntime;
import static java.lang.System.arraycopy;
import static java.time.Duration.between;
import static java.time.Instant.now;
import static java.util.concurrent.Executors.newWorkStealingPool;
import static java.util.concurrent.ForkJoinPool.commonPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.trim;
import static org.apache.hop.core.util.Utils.isEmpty;
import static org.apache.hop.pipeline.transforms.languagemodelchat.internals.ui.i18nUtil.i18n;

import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.data.message.ImageContent;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.chat.response.ChatResponse;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hop.ai.provider.AiChatCapability;
import org.apache.hop.ai.provider.AiChatMessage;
import org.apache.hop.ai.provider.AiChatOptions;
import org.apache.hop.ai.provider.AiChatProviderSession;
import org.apache.hop.ai.provider.AiChatProxy;
import org.apache.hop.ai.provider.AiChatRequest;
import org.apache.hop.ai.provider.AiChatResponse;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.languagemodelchat.internals.ChatMessages;
import org.apache.hop.pipeline.transforms.languagemodelchat.internals.LanguageModel;
import org.apache.hop.pipeline.transforms.languagemodelchat.internals.LanguageModelFacade;
import org.apache.hop.pipeline.transforms.languagemodelchat.internals.ModelType;
import org.apache.hop.pipeline.transforms.languagemodelchat.internals.OllamaThink;

public class LanguageModelChat extends BaseTransform<LanguageModelChatMeta, LanguageModelChatData> {
  public static final int OVER_ALLOCATE_SIZE = 10;
  private static final Class<?> PKG = LanguageModelChat.class; // For Translator
  public static final String CONST_LANGUAGE_MODEL_CHAT_EXCEPTION_COULDNOT_FIND_FIELD =
      "LanguageModelChat.Exception.CouldnotFindField";
  public static final String CONST_MODEL_TYPE = "model_type";

  private Map<LanguageModel, LanguageModelFacade> facadeMap = new ConcurrentHashMap<>();
  private LanguageModelChatMeta modelMeta;
  private AiChatProviderSession nativeChatSession;

  private int parallelism = 1;
  private ForkJoinPool executor;
  private final int cores = Runtime.getRuntime().availableProcessors();
  private final AtomicInteger jobs = new AtomicInteger(0);
  private final int maxJobs = cores * 100;
  private final Object lock = new Object();

  public LanguageModelChat(
      TransformMeta transformMeta,
      LanguageModelChatMeta meta,
      LanguageModelChatData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  private void finish() {

    synchronized (lock) {
      while (jobs.get() > 0) {
        try {
          lock.wait();
        } catch (InterruptedException e) {
          throw new HopRuntimeException(e);
        }
      }
    }

    if (executor != null) {
      try {
        while (executor.hasQueuedSubmissions() || executor.getActiveThreadCount() > 0) {
          SECONDS.sleep(5);
        }
      } catch (InterruptedException e) {
        logError(e.getMessage(), e);
      }
    }

    facadeMap.clear();
    setOutputDone();
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] r = getRow(); // Get row from input rowset & set row busy!
    if (r == null) { // no more input to be expected...
      finish();
      return false;
    }

    if (first) {
      first = false;

      // get the RowMeta
      data.previousRowMeta = getInputRowMeta().clone();
      data.nrPrevFields = data.previousRowMeta.size();
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

      nativeChatSession =
          LanguageModelChatAiProviderSupport.resolveNative(
              meta.getAiProviderName(), this, metadataProvider);
      if (nativeChatSession == null) {
        modelMeta = LanguageModelChatAiProviderSupport.resolve(meta, this, metadataProvider);
      } else {
        modelMeta = (LanguageModelChatMeta) meta.clone();
        modelMeta.setModelType(ModelType.OPEN_AI.code());
        modelMeta.setOpenAiModelName(nativeChatSession.modelName());
        validateNativeInlineConnectionSettings();
      }
      facadeMap.put(new LanguageModel(modelMeta), new LanguageModelFacade(variables, modelMeta));

      int parallelism =
          meta.getParallelism() <= 0 ? getRuntime().availableProcessors() : meta.getParallelism();
      if (parallelism != 0 && this.parallelism != parallelism || executor == null) {
        if (executor != null) {
          executor.shutdownNow();
        }
        executor = (ForkJoinPool) newWorkStealingPool(parallelism);
      }

      if (isEmpty(meta.getInputField())) {
        throw new HopException(
            BaseMessages.getString(PKG, "LanguageModelChat.Error.InputFieldMissing"));
      }
      // cache the position of the field
      cacheIndexPositions();
    } // End If first

    try {
      process(r);

      if (isRowLevel()) {
        logRowlevel(
            i18n(
                "LanguageModelChat.LineNumber",
                getLinesRead() + " : " + getInputRowMeta().getString(r)));
      }
    } catch (Exception e) {

      String errorMessage;
      if (getTransformMeta().isDoingErrorHandling()) {
        errorMessage = e.toString();
      } else {
        logError(i18n("LanguageModelChat.ErrorInTransformRunning") + e.getMessage());
        setErrors(1);
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
      // Simply add this row to the error row
      putError(getInputRowMeta(), r, 1, errorMessage, meta.getInputField(), "LanguageModelChat001");
    }

    return true;
  }

  private void process(Object[] inputRow) {
    String message = trim((String) inputRow[data.indexOfInputField]);
    if (isBlank(message)) {
      return;
    }

    synchronized (lock) {
      while (jobs.get() > maxJobs) {
        try {
          lock.wait();
        } catch (InterruptedException e) {
          throw new HopRuntimeException(e);
        }
      }
    }

    ExecutorService exec = meta.getParallelism() == 0 ? commonPool() : executor;

    exec.submit(
        () -> {
          try {
            processSync(inputRow, message);
          } catch (Exception e) {
            try {
              if (getTransformMeta().isDoingErrorHandling()) {
                String errorMessage = e.toString();
                putError(
                    getInputRowMeta(),
                    inputRow,
                    1L,
                    errorMessage,
                    meta.getInputField(),
                    "LanguageModelChat001");
              } else {
                logError(i18n("LanguageModelChat.ErrorInTransformRunning") + e.getMessage(), e);
                setErrors(1L);
                stopAll();
              }
            } catch (HopTransformException ex) {
              logError(e.getMessage(), e);
              logError(ex.getMessage(), ex);
              if (!getTransformMeta().isDoingErrorHandling()) {
                setErrors(1L);
                stopAll();
              }
            }
          } finally {
            jobs.decrementAndGet();
            synchronized (lock) {
              lock.notifyAll();
            }
          }
        });
    jobs.incrementAndGet();
  }

  private void processSync(Object[] inputRow, String message)
      throws HopTransformException, HopValueException {

    Long inputTokenCount = null;
    Long outputTokenCount = null;
    Long totalTokenCount = null;
    Long inferenceTime = null;
    String finishReason = null;
    String output = null;

    LanguageModel model = new LanguageModel(modelMeta);
    LanguageModelFacade facade = facadeMap.get(model);

    List<ChatMessage> messageList = facade.inputToChatMessages(message);

    if (meta.isMock()) {
      inputTokenCount = -1L;
      outputTokenCount = -1L;
      totalTokenCount = -1L;
      inferenceTime = -1L;
      finishReason = "MOCK";

      if (meta.isOutputChatJson()) {
        output = facade.messagesToOutput(messageList, meta.getMockOutputValue());
      } else {
        output = meta.getMockOutputValue();
      }
    } else {
      Instant inferenceStart = now();
      try {
        String responseText;
        if (nativeChatSession != null) {
          AiChatResponse ai =
              nativeChatSession.chat(nativeRequest(messageList, nativeRequestOptions()), variables);
          inferenceTime = ai.durationMs();
          inputTokenCount = ai.inputTokenCount() == null ? null : ai.inputTokenCount().longValue();
          outputTokenCount =
              ai.outputTokenCount() == null ? null : ai.outputTokenCount().longValue();
          totalTokenCount =
              ai.inputTokenCount() == null || ai.outputTokenCount() == null
                  ? null
                  : (long) ai.inputTokenCount() + ai.outputTokenCount();
          finishReason = ai.finishReason();
          responseText = ai.text();
        } else {
          ChatResponse ai = facade.chat(messageList);
          inferenceTime = between(inferenceStart, now()).toMillis();
          inputTokenCount =
              ai.tokenUsage() == null || ai.tokenUsage().inputTokenCount() == null
                  ? null
                  : ai.tokenUsage().inputTokenCount().longValue();
          outputTokenCount =
              ai.tokenUsage() == null || ai.tokenUsage().outputTokenCount() == null
                  ? null
                  : ai.tokenUsage().outputTokenCount().longValue();
          totalTokenCount =
              ai.tokenUsage() == null || ai.tokenUsage().totalTokenCount() == null
                  ? null
                  : ai.tokenUsage().totalTokenCount().longValue();
          finishReason = ai.finishReason() == null ? null : ai.finishReason().name();
          responseText = ai.aiMessage().text();
        }
        if (meta.isOutputChatJson()) {
          output = facade.messagesToOutput(messageList, responseText);
        } else {
          output = responseText;
        }
      } catch (Exception e) {
        inferenceTime = between(inferenceStart, now()).toMillis();
        finishReason = "ERROR";
        output = e.getMessage();
      }
    }

    int newFields = 8;
    Object[] outputRow = new Object[inputRow.length + newFields + OVER_ALLOCATE_SIZE];
    arraycopy(inputRow, 0, outputRow, 0, inputRow.length);

    String outputModelName =
        nativeChatSession == null ? model.getName() : nativeChatSession.modelName();
    outputRow[data.indexOfIdentifier] =
        isNotBlank(meta.getIdentifierValue()) ? meta.getIdentifierValue() : outputModelName;
    outputRow[data.indexOfModelType] =
        nativeChatSession == null ? model.getType().code() : nativeChatSession.modelType();
    outputRow[data.indexOfModelName] = outputModelName;
    outputRow[data.indexOfFinishReason] = finishReason;
    outputRow[data.indexOfInputTokenCount] = inputTokenCount;
    outputRow[data.indexOfOutputTokenCount] = outputTokenCount;
    outputRow[data.indexOfTotalTokenCount] = totalTokenCount;
    outputRow[data.indexOfInferenceTime] = inferenceTime;
    outputRow[data.indexOfOutput] = output;

    putRow(data.outputRowMeta, outputRow);
  }

  private AiChatRequest nativeRequest(List<ChatMessage> messages, AiChatOptions options)
      throws HopValueException {
    List<AiChatMessage> conversation = new ArrayList<>();
    String userPrompt = "";
    int userIndex = messages.size() - 1;
    if (userIndex >= 0 && messages.get(userIndex) instanceof UserMessage) {
      userPrompt = nativeMessageText(messages.get(userIndex));
    } else {
      userIndex = messages.size();
    }
    for (int i = 0; i < userIndex; i++) {
      ChatMessage message = messages.get(i);
      AiChatMessage.Role role =
          switch (message.type()) {
            case SYSTEM -> AiChatMessage.Role.SYSTEM;
            case USER -> AiChatMessage.Role.USER;
            case AI -> AiChatMessage.Role.ASSISTANT;
            default ->
                throw new HopValueException(
                    "The selected native AI provider does not support tool messages.");
          };
      conversation.add(new AiChatMessage(role, nativeMessageText(message)));
    }
    boolean jsonRequested = nativeJsonRequested();
    boolean jsonOnly =
        jsonRequested
            && nativeChatSession
                .provider()
                .capabilities()
                .contains(AiChatCapability.JSON_ONLY_PROMPT);
    AiChatOptions effectiveOptions =
        new AiChatOptions(
            options.temperature(),
            options.topP(),
            options.topK(),
            options.contextSize(),
            options.maxOutputTokens(),
            options.thinking(),
            options.seed(),
            options.presencePenalty(),
            options.frequencyPenalty(),
            options.maxRetries(),
            options.structuredOutput() || (jsonRequested && !jsonOnly),
            options.proxy());
    return new AiChatRequest(
        nativeChatSession.modelName(),
        null,
        userPrompt,
        conversation,
        null,
        jsonOnly,
        effectiveOptions);
  }

  private String nativeMessageText(ChatMessage message) throws HopValueException {
    if (message instanceof UserMessage userMessage
        && userMessage.contents().stream().anyMatch(ImageContent.class::isInstance)) {
      throw new HopValueException(
          "The selected native AI provider does not support image input in Language Model Chat.");
    }
    return ChatMessages.text(message);
  }

  private AiChatOptions nativeRequestOptions() {
    ModelType type = ModelType.valueOf(meta.getModelType());
    return switch (type) {
      case OPEN_AI ->
          new AiChatOptions(
              different(meta.getOpenAiTemperature(), 0.7),
              meta.getOpenAiTopP(),
              null,
              null,
              meta.getOpenAiMaxTokens(),
              null,
              meta.getOpenAiSeed(),
              meta.getOpenAiPresencePenalty(),
              meta.getOpenAiFrequencyPenalty(),
              different(meta.getOpenAiMaxRetries(), 3),
              false,
              meta.isOpenAiUseProxy()
                  ? new AiChatProxy(meta.getOpenAiProxyHost(), meta.getOpenAiProxyPort())
                  : null);
      case ANTHROPIC ->
          new AiChatOptions(
              meta.getAnthropicTemperature(),
              meta.getAnthropicTopP(),
              meta.getAnthropicTopK(),
              null,
              different(meta.getAnthropicMaxTokens(), 1024),
              null,
              null,
              null,
              null,
              different(meta.getAnthropicMaxRetries(), 3),
              false);
      case OLLAMA ->
          new AiChatOptions(
              meta.getOllamaTemperature(),
              meta.getOllamaTopP(),
              meta.getOllamaTopK(),
              meta.getOllamaNumCtx(),
              meta.getOllamaNumPredict(),
              OllamaThink.think(meta.getOllamaThink()),
              meta.getOllamaSeed(),
              null,
              null,
              different(meta.getOllamaMaxRetries(), 3),
              isJson(meta.getOllamaFormat()));
      case MISTRAL ->
          new AiChatOptions(
              meta.getMistralTemperature(),
              meta.getMistralTopP(),
              null,
              null,
              meta.getMistralMaxTokens(),
              null,
              meta.getMistralRandomSeed(),
              null,
              null,
              different(meta.getMistralMaxRetries(), 3),
              isJson(meta.getMistralResponseFormat()));
      case HUGGING_FACE ->
          new AiChatOptions(
              meta.getHuggingFaceTemperature(),
              null,
              null,
              null,
              meta.getHuggingFaceMaxNewTokens(),
              null,
              null,
              null,
              null,
              null,
              false);
    };
  }

  private boolean nativeJsonRequested() {
    return switch (ModelType.valueOf(meta.getModelType())) {
      case OPEN_AI -> isJson(meta.getOpenAiResponseFormat());
      case OLLAMA -> isJson(meta.getOllamaFormat());
      case MISTRAL -> isJson(meta.getMistralResponseFormat());
      default -> false;
    };
  }

  private static boolean isJson(String format) {
    return format != null && format.toLowerCase().contains("json");
  }

  private static <T> T different(T value, T defaultValue) {
    return java.util.Objects.equals(value, defaultValue) ? null : value;
  }

  private void validateNativeInlineConnectionSettings() throws HopException {
    List<String> configured = new ArrayList<>();
    switch (ModelType.valueOf(meta.getModelType())) {
      case OPEN_AI -> {
        if (different(meta.getOpenAiBaseUrl(), "https://api.openai.com/v1") != null) {
          configured.add("OpenAI base URL");
        }
        if (different(meta.getOpenAiApiKey(), "OPENAI_API_KEY") != null) {
          configured.add("OpenAI API key");
        }
        if (isNotBlank(meta.getOpenAiOrganizationId()) || isNotBlank(meta.getOpenAiUser())) {
          configured.add("OpenAI organization or user");
        }
        if (meta.isOpenAiLogRequests() || meta.isOpenAiLogResponses()) {
          configured.add("OpenAI request/response logging");
        }
        if (different(meta.getOpenAiTimeout(), 60) != null) {
          configured.add("OpenAI timeout");
        }
      }
      case ANTHROPIC -> {
        if (different(meta.getAnthropicBaseUrl(), "https://api.anthropic.com/v1/") != null
            || different(meta.getAnthropicApiKey(), "ANTHROPIC_API_KEY") != null
            || different(meta.getAnthropicVersion(), "2023-06-01") != null) {
          configured.add("Anthropic connection settings");
        }
        if (meta.isAnthropicLogRequests() || meta.isAnthropicLogResponses()) {
          configured.add("Anthropic request/response logging");
        }
        if (different(meta.getAnthropicTimeout(), 15) != null) {
          configured.add("Anthropic timeout");
        }
      }
      case MISTRAL -> {
        if (different(meta.getMistralBaseUrl(), "https://api.mistral.ai/v1") != null
            || different(meta.getMistralApiKey(), "MISTRAL_API_KEY") != null) {
          configured.add("Mistral connection settings");
        }
        if (!meta.isMistralSafePrompt()
            || meta.isMistralLogRequests()
            || meta.isMistralLogResponses()) {
          configured.add("Mistral safe prompt or request/response logging");
        }
        if (different(meta.getMistralTimeout(), 60) != null) {
          configured.add("Mistral timeout");
        }
      }
      case HUGGING_FACE -> {
        if (different(meta.getHuggingFaceAccessToken(), "HF_ACCESS_TOKEN") != null) {
          configured.add("Hugging Face access token");
        }
        if (meta.isHuggingFaceReturnFullText() || !meta.isHuggingFaceWaitForModel()) {
          configured.add("Hugging Face response options");
        }
        if (different(meta.getHuggingFaceTimeout(), 15) != null) {
          configured.add("Hugging Face timeout");
        }
      }
      case OLLAMA -> {
        if (isNotBlank(meta.getOllamaImageEndpoint())) {
          configured.add("Ollama endpoint");
        }
        if (meta.getOllamaRepeatPenalty() != null) {
          configured.add("Ollama repeat penalty");
        }
        if (different(meta.getOllamaTimeout(), 60) != null) {
          configured.add("Ollama timeout");
        }
      }
    }
    if (!configured.isEmpty()) {
      throw new HopException(
          "A named native AI provider supplies its own connection settings; remove unsupported inline "
              + String.join(", ", configured)
              + " from Language Model Chat.");
    }
  }

  private void cacheIndexPositions() throws HopException {
    if (data.indexOfInputField < 0) {
      data.indexOfInputField = data.previousRowMeta.indexOfValue(meta.getInputField());
      if (data.indexOfInputField < 0) {
        throw new HopException(
            i18n(CONST_LANGUAGE_MODEL_CHAT_EXCEPTION_COULDNOT_FIND_FIELD, meta.getInputField()));
      }
    }
    String outputPrefix = meta.getOutputFieldNamePrefix();

    if (data.indexOfIdentifier < 0) {
      data.indexOfIdentifier = data.outputRowMeta.indexOfValue(outputPrefix + "identifier");
      if (data.indexOfIdentifier < 0) {
        throw new HopException(
            i18n(CONST_LANGUAGE_MODEL_CHAT_EXCEPTION_COULDNOT_FIND_FIELD, CONST_MODEL_TYPE));
      }
    }
    if (data.indexOfModelType < 0) {
      data.indexOfModelType = data.outputRowMeta.indexOfValue(outputPrefix + CONST_MODEL_TYPE);
      if (data.indexOfModelType < 0) {
        throw new HopException(
            i18n(CONST_LANGUAGE_MODEL_CHAT_EXCEPTION_COULDNOT_FIND_FIELD, CONST_MODEL_TYPE));
      }
    }
    if (data.indexOfModelName < 0) {
      data.indexOfModelName = data.outputRowMeta.indexOfValue(outputPrefix + "model_name");
      if (data.indexOfModelName < 0) {
        throw new HopException(
            i18n(CONST_LANGUAGE_MODEL_CHAT_EXCEPTION_COULDNOT_FIND_FIELD, "model_name"));
      }
    }
    if (data.indexOfFinishReason < 0) {
      data.indexOfFinishReason = data.outputRowMeta.indexOfValue(outputPrefix + "finish_reason");
      if (data.indexOfFinishReason < 0) {
        throw new HopException(
            i18n(CONST_LANGUAGE_MODEL_CHAT_EXCEPTION_COULDNOT_FIND_FIELD, "finish_reason"));
      }
    }
    if (data.indexOfInputTokenCount < 0) {
      data.indexOfInputTokenCount =
          data.outputRowMeta.indexOfValue(outputPrefix + "input_token_count");
      if (data.indexOfInputTokenCount < 0) {
        throw new HopException(
            i18n(CONST_LANGUAGE_MODEL_CHAT_EXCEPTION_COULDNOT_FIND_FIELD, "input_token_count"));
      }
    }
    if (data.indexOfOutputTokenCount < 0) {
      data.indexOfOutputTokenCount =
          data.outputRowMeta.indexOfValue(outputPrefix + "output_token_count");
      if (data.indexOfOutputTokenCount < 0) {
        throw new HopException(
            i18n(CONST_LANGUAGE_MODEL_CHAT_EXCEPTION_COULDNOT_FIND_FIELD, "output_token_count"));
      }
    }
    if (data.indexOfTotalTokenCount < 0) {
      data.indexOfTotalTokenCount =
          data.outputRowMeta.indexOfValue(outputPrefix + "total_token_count");
      if (data.indexOfTotalTokenCount < 0) {
        throw new HopException(
            i18n(CONST_LANGUAGE_MODEL_CHAT_EXCEPTION_COULDNOT_FIND_FIELD, "total_token_count"));
      }
    }
    if (data.indexOfInferenceTime < 0) {
      data.indexOfInferenceTime = data.outputRowMeta.indexOfValue(outputPrefix + "inference_time");
      if (data.indexOfInferenceTime < 0) {
        throw new HopException(
            i18n(CONST_LANGUAGE_MODEL_CHAT_EXCEPTION_COULDNOT_FIND_FIELD, "inference_time"));
      }
    }
    if (data.indexOfOutput < 0) {
      data.indexOfOutput = data.outputRowMeta.indexOfValue(outputPrefix + "output");
      if (data.indexOfOutput < 0) {
        throw new HopException(
            i18n(CONST_LANGUAGE_MODEL_CHAT_EXCEPTION_COULDNOT_FIND_FIELD, "output"));
      }
    }
  }

  @Override
  public void putRow(IRowMeta rowMeta, Object[] row) throws HopTransformException {
    synchronized (this) {
      super.putRow(rowMeta, row);
    }
  }

  @Override
  public void putError(
      IRowMeta rowMeta,
      Object[] row,
      long nrErrors,
      String errorDescriptions,
      String fieldNames,
      String errorCodes)
      throws HopTransformException {
    synchronized (this) {
      super.putError(rowMeta, row, nrErrors, errorDescriptions, fieldNames, errorCodes);
    }
  }
}
