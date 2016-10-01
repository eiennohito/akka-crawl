package org.jsoup.helper;

import org.jsoup.nodes.Document;
import org.jsoup.parser.Parser;

import java.nio.ByteBuffer;

/**
 * @author eiennohito
 * @since 2016/09/23
 */
public class JsoupUtil {
  public static Document fromBuffer(ByteBuffer buffer, String charset, String uri, Parser parser) {
    int pos = buffer.position();
    Document document = DataUtil.parseByteData(buffer, charset, uri, parser);
    buffer.position(pos);
    return document;
  }
}
