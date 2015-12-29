/*
 * Copyright 2011 the original author or authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pivotal.gemfirexd.internal.engine.jayway.jsonpath.internal;

public class Parser {

    public enum Token {
        DOT('.'),
        OPEN_BRACKET('['),
        CLOSE_BRACKET(']'),
        OPEN_PARENTHESIS('('),
        CLOSE_PARENTHESIS(')'),
        BLANK(' '),
        TICK('\''),
        END('^');

        private final char c;

        Token(char c) {
            this.c = c;
        }
    }


    protected String buffer;
    private int i;

    public Parser(String src) {
        buffer = src;
        i = -1;
    }

    public char prev(){
        return buffer.charAt(i-1);
    }

    public boolean prevIs(char c){
        if(i <= 0){
            return false;
        }
        return prev() == c;
    }

    public char curr(){
        return buffer.charAt(i);
    }

    public char peek(){
        return buffer.charAt(i + 1);
    }

    public boolean peekIs(Token token){
        return buffer.charAt(i+1) == token.c;
    }

    public char next(){
        return buffer.charAt(++i);
    }

    public boolean hasNext(){
        return i < buffer.length() - 1;
    }

    public String next(int count){
        //i++;
        int y = i;
        i = i + count - 1;
        return buffer.substring(y, i+1);
        //return Arrays.copyOfRange(buffer, y, i+1);
    }

    public void trim(Token token){
       while (peekIs(token)){
           next();
       }
    }

    public int findOffset(Token... tokens){
        int y = i;
        char check;
        do {
            if(y == buffer.length()-1 && contains(tokens, Token.END.c)){
                y++;
                break;
            }
            check = buffer.charAt(++y);
        } while (!contains(tokens, check));

        return y-i;
    }

    public String nextUntil(Token... tokens){
        next();
        int offset = findOffset(tokens);
        return next(offset);
    }

    public boolean isInts(String chars, boolean allowSequence){
        for (int i = 0; i < chars.length(); i++){

            char c = chars.charAt(i);

            boolean isSequenceChar = (c == ' ' || c == ',');

            if(!Character.isDigit(c) || (isSequenceChar && allowSequence)){
                return false;
            }
        }
        return true;
    }


    private boolean contains(Token[] arr, char checkFor){
        for (int i = 0; i < arr.length; i++){
            if(arr[i].c == checkFor){
                return true;
            }
        }
        return false;
    }



        /*
    public Filter2 parse(String filterString){

        char[] chars = filterString.trim().toCharArray();
        int i = 0;


        do {
            char current = chars[i];
            switch (current){
                case '?':
                    break;

                case '(':
                    break;

                case ')':
                    break;

                case '\'';
            }


            i++;
        } while (i < chars.length);








    }
    */
}
