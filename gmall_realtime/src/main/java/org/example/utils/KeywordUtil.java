package org.example.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * 分词工具类
 */
public class KeywordUtil {

    public static List<String> splitKeyWord(String keyword) throws Exception {
        //创建集合用于存放结果集
        ArrayList<String> resultList = new ArrayList<>();

        StringReader reader = new StringReader(keyword);

        IKSegmenter ikSegmenter = new IKSegmenter(reader, false);

        while (true) {
            Lexeme next = ikSegmenter.next();
            if (next != null) {
                String word = next.getLexemeText();
                resultList.add(word);
            } else {
                break;
            }
        }
        return resultList;
    }
}
