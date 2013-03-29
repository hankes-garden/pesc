clc;clear all;close all;
path = 'F:\yl';%%·��
index_file = 'clusterResult.txt';%%�ļ���
filename = fullfile(path,index_file);
M = importdata(filename);
[row,col] = size(M.data);

fs = 300;

 %% ��ý�����ɫRGBֵ
color = hsv(row);

for i = 1:row
    ecg_file = char(M.rowheaders(i));
    filename = fullfile(path,ecg_file);
    fid = fopen(filename,'r+');
    ecg_data = fread(fid,'int16');
    fclose(fid);
%     plot(ecg_data);

    begin = M.data(i,1);
    endl = M.data(i,2) -30;
    R_pos = M.data(i,3);
    q_pos = M.data(i,4);
    baseline = mean(ecg_data(q_pos-0.02*fs:q_pos));%%��q��ǰ20ms��ƽ��ֵΪ����
    
    
    segment = ecg_data(begin:endl)-baseline;
    x = begin-R_pos:endl-R_pos;
    invalid = 0;
    
    maxVal = max(segment);
    minVal = min(segment);
    if ((maxVal > 1300) || (minVal < -400))
            invalid = 1;
            continue;
    end

    
    plot(x,segment,'color',color(i,:));hold on;
end


