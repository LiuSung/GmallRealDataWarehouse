package com.gmall.realtime.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeywordUtil {
    public static List<String> splitKeyword(String keyword) throws IOException {

        ArrayList<String> list = new ArrayList<>();

        IKSegmenter ikSegmenter = new IKSegmenter(new StringReader(keyword),true);
        Lexeme next = ikSegmenter.next();
        while (next != null){
            String word = next.getLexemeText();
            list.add(word);
            next = ikSegmenter.next();
        }
        return list;
    }
}
