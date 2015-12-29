/*
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
/**
 * 
 */
package wanActiveActive;

import java.awt.BorderLayout;
import java.awt.Button;
import java.awt.Frame;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Label;
import java.awt.Panel;
import java.awt.TextField;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;

import javax.swing.JFrame;

/**
 * A simple AWT dialog used by this example to show conflict resolution and the
 * current state of the cache in each process.
 */
public class Dialog implements ActionListener {
  /**
   * A callback that is invoked when the "Close" button is pressed or the dialog
   * is otherwise terminated
   */
  CloseCallback closer;

  String title;
  
  Frame myFrame;
  Label modification;
  TextField history;
  
  boolean includeConflictCount;
  TextField conflictCount;
  
  public Dialog(String title, CloseCallback c, boolean includeConflictCount) {
    this.title = title;
    this.closer = c;
    this.includeConflictCount = includeConflictCount;
  }

  /**
   * Set the history text of the dialog.  The dialog must be opened prior to
   * invoking this method.
   */
  public void setHistory(String text) {
    this.history.setText(text);
  }
  
  /**
   * Set the cache content text of this dialog.  The dialog must be opened prior
   * to invoking this method.
   */
  public void setModification(String text) {
    this.modification.setText("Cache state: " + text);
  }
  
  

  /**
   * Set the conflictCount text of the dialog.  The dindow must be opened prior
   * to invoking this method.
   */
  public void setConflictCount(int count) {
    if (this.conflictCount != null) {
      this.conflictCount.setText("Conflict count: " + count);
    }
  }
  
  public void open() {
      Frame frm=new JFrame(title);
      frm.setSize(700, 115);
      
      Panel p = new Panel();
      p.setLayout(new GridBagLayout());

      GridBagConstraints c = new GridBagConstraints();
      this.modification = new Label("Cache state: ");
      c.anchor = GridBagConstraints.WEST;
      c.fill = GridBagConstraints.HORIZONTAL;
      c.gridx = 0;
      c.gridy = 0;
      p.add(this.modification, c);
      
      this.history = new TextField(80);
      c = new GridBagConstraints();
      c.anchor = GridBagConstraints.WEST;
      c.fill = GridBagConstraints.HORIZONTAL;
      c.gridx = 0;
      c.gridy = 1;
      p.add(this.history, c);

      int y = 1;
      
      if (this.includeConflictCount) {
        this.conflictCount = new TextField("Conflict count: 0", 25);
        c = new GridBagConstraints();
        c.anchor = GridBagConstraints.WEST;
        c.fill = GridBagConstraints.NONE;
        y += 1;
        c.gridy = y;
        p.add(this.conflictCount, c);
      }
      
      Button close=new Button("Close");
      c = new GridBagConstraints();
      c.anchor = GridBagConstraints.WEST;
      c.fill = GridBagConstraints.NONE;
      y += 1;
      c.gridy = y;
      p.add(close, c);
      close.addActionListener(this);

      Panel p3 = new Panel();
      p3.add(p);
      frm.add(p3,BorderLayout.NORTH);

      frm.pack();
      frm.setVisible(true);
      
      frm.addWindowListener(new WindowAdapter(){
        public void windowClosing(WindowEvent e){
          closer.close();
        }
      });
  }
  
  /* (non-Javadoc)
   * @see java.awt.event.ActionListener#actionPerformed(java.awt.event.ActionEvent)
   */
  @Override
  public void actionPerformed(ActionEvent e) {
    this.closer.close();
  }

  public interface CloseCallback {
    void close();
  }
}
