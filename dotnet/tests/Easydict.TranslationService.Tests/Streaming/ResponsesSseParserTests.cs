using System.Text;
using Easydict.TranslationService.Streaming;
using FluentAssertions;
using Xunit;

namespace Easydict.TranslationService.Tests.Streaming;

public class ResponsesSseParserTests
{
    private static Stream CreateSseStream(string content) =>
        new MemoryStream(Encoding.UTF8.GetBytes(content));

    [Fact]
    public async Task ParseStreamAsync_YieldsDelta_FromNamedEvents()
    {
        var sseContent = """
            event: response.output_text.delta
            data: {"type":"response.output_text.delta","delta":"Hello"}

            event: response.output_text.delta
            data: {"type":"response.output_text.delta","delta":" World"}

            data: [DONE]

            """;
        using var stream = CreateSseStream(sseContent);

        var chunks = new List<string>();
        await foreach (var chunk in ResponsesSseParser.ParseStreamAsync(stream))
        {
            chunks.Add(chunk);
        }

        chunks.Should().Equal("Hello", " World");
    }

    [Fact]
    public async Task ParseStreamAsync_YieldsDelta_FromTypeFieldWithoutEventLine()
    {
        var sseContent = """
            data: {"type":"response.output_text.delta","delta":"Hello"}

            data: {"type":"response.completed","status":"completed"}

            data: [DONE]

            """;
        using var stream = CreateSseStream(sseContent);

        var chunks = new List<string>();
        await foreach (var chunk in ResponsesSseParser.ParseStreamAsync(stream))
        {
            chunks.Add(chunk);
        }

        chunks.Should().ContainSingle()
            .Which.Should().Be("Hello");
    }
}
