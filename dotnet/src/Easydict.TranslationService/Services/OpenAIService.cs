using System.Net.Http.Headers;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using Easydict.TranslationService.Models;
using Easydict.TranslationService.Streaming;

namespace Easydict.TranslationService.Services;

/// <summary>
/// OpenAI translation service using the Responses API by default.
/// Requires API key from user settings.
/// </summary>
public sealed class OpenAIService : BaseOpenAIService
{
    public const string DefaultEndpoint = "https://api.openai.com/v1/responses";
    public const string LegacyChatCompletionsEndpoint = "https://api.openai.com/v1/chat/completions";
    private const string DefaultModel = "gpt-4o-mini";

    /// <summary>
    /// Available OpenAI models for translation.
    /// </summary>
    public static readonly string[] AvailableModels = new[]
    {
        "gpt-4o-mini",
        "gpt-4o",
        "gpt-4-turbo",
        "gpt-3.5-turbo"
    };

    private string _endpoint = DefaultEndpoint;
    private string _apiKey = "";
    private string _model = DefaultModel;
    private double _temperature = 0.3;

    public OpenAIService(HttpClient httpClient) : base(httpClient) { }

    public override string ServiceId => "openai";
    public override string DisplayName => "OpenAI";
    public override bool RequiresApiKey => true;
    public override bool IsConfigured => !string.IsNullOrEmpty(_apiKey);
    public override IReadOnlyList<Language> SupportedLanguages => OpenAILanguages;

    public override string Endpoint => _endpoint;
    public override string ApiKey => _apiKey;
    public override string Model => _model;
    public override double Temperature => _temperature;

    public override async IAsyncEnumerable<string> TranslateStreamAsync(
        TranslationRequest request,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (!UsesResponsesEndpoint(Endpoint))
        {
            await foreach (var chunk in base.TranslateStreamAsync(request, cancellationToken).ConfigureAwait(false))
            {
                yield return chunk;
            }

            yield break;
        }

        ValidateConfiguration();

        var requestBody = BuildResponsesRequestBody(BuildChatMessages(request));
        await foreach (var chunk in StreamResponsesAsync(requestBody, cancellationToken).ConfigureAwait(false))
        {
            yield return chunk;
        }
    }

    public override async IAsyncEnumerable<string> CorrectGrammarStreamAsync(
        GrammarCorrectionRequest request,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (!UsesResponsesEndpoint(Endpoint))
        {
            await foreach (var chunk in base.CorrectGrammarStreamAsync(request, cancellationToken).ConfigureAwait(false))
            {
                yield return chunk;
            }

            yield break;
        }

        ValidateConfiguration();

        var requestBody = BuildResponsesRequestBody(BuildGrammarCorrectionMessages(request));
        await foreach (var chunk in StreamResponsesAsync(requestBody, cancellationToken).ConfigureAwait(false))
        {
            yield return chunk;
        }
    }

    /// <summary>
    /// Configure the OpenAI service.
    /// </summary>
    /// <param name="apiKey">OpenAI API key (required).</param>
    /// <param name="endpoint">Custom endpoint URL (optional, defaults to OpenAI API).</param>
    /// <param name="model">Model to use (optional, defaults to gpt-4o-mini).</param>
    /// <param name="temperature">Generation temperature (optional, defaults to 0.3).</param>
    public void Configure(string apiKey, string? endpoint = null, string? model = null, double? temperature = null)
    {
        _apiKey = apiKey ?? "";
        if (!string.IsNullOrEmpty(endpoint))
            _endpoint = endpoint;
        if (!string.IsNullOrEmpty(model))
            _model = model;
        if (temperature.HasValue)
            _temperature = Math.Clamp(temperature.Value, 0.0, 2.0);
    }

    private object BuildResponsesRequestBody(List<ChatMessage> messages)
    {
        var instructions = messages.FirstOrDefault(m => m.Role == ChatRole.System)?.Content;
        var input = string.Join(
            "\n\n",
            messages.Where(m => m.Role != ChatRole.System).Select(m => m.Content));

        return new
        {
            model = Model,
            instructions,
            input,
            temperature = Temperature,
            stream = true,
            store = false
        };
    }

    private async IAsyncEnumerable<string> StreamResponsesAsync(
        object requestBody,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using var httpRequest = new HttpRequestMessage(HttpMethod.Post, Endpoint);
        httpRequest.Content = new StringContent(
            JsonSerializer.Serialize(requestBody),
            Encoding.UTF8,
            "application/json");

        if (!string.IsNullOrEmpty(ApiKey))
        {
            httpRequest.Headers.Authorization = new AuthenticationHeaderValue("Bearer", ApiKey);
        }

        ConfigureHttpRequest(httpRequest);

        HttpResponseMessage response;
        try
        {
            response = await HttpClient.SendAsync(
                httpRequest,
                HttpCompletionOption.ResponseHeadersRead,
                cancellationToken).ConfigureAwait(false);
        }
        catch (HttpRequestException ex)
        {
            throw new TranslationException($"Network error: {ex.Message}", ex)
            {
                ErrorCode = TranslationErrorCode.NetworkError,
                ServiceId = ServiceId
            };
        }

        using (response)
        {
            if (!response.IsSuccessStatusCode)
            {
                var errorBody = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
                throw CreateErrorFromResponse(response.StatusCode, errorBody);
            }

            var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
            await foreach (var chunk in ResponsesSseParser.ParseStreamAsync(stream, cancellationToken).ConfigureAwait(false))
            {
                yield return chunk;
            }
        }
    }

    private static bool UsesResponsesEndpoint(string endpoint)
    {
        return Uri.TryCreate(endpoint, UriKind.Absolute, out var uri) &&
               uri.AbsolutePath.EndsWith("/responses", StringComparison.OrdinalIgnoreCase);
    }
}
