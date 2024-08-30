# Ultralytics YOLO 🚀, AGPL-3.0 license

from ultralytics.engine.predictor import BasePredictor
from ultralytics.engine.results import Results
from ultralytics.utils import ops

from keras.models import load_model
import numpy as np
import cv2
import matplotlib.pyplot as plt


class DetectionPredictor(BasePredictor):
    """
    A class extending the BasePredictor class for prediction based on a detection model.

    Example:
        ```python
        from ultralytics.utils import ASSETS
        from ultralytics.models.yolo.detect import DetectionPredictor

        args = dict(model='yolov8n.pt', source=ASSETS)
        predictor = DetectionPredictor(overrides=args)
        predictor.predict_cli()
        ```
    """

    def postprocess(self, preds, img, orig_imgs):
        """Post-processes predictions and returns a list of Results objects."""
        preds = ops.non_max_suppression(
            preds,
            self.args.conf,
            self.args.iou,
            agnostic=self.args.agnostic_nms,
            max_det=self.args.max_det,
            classes=self.args.classes,
        )

        if not isinstance(orig_imgs, list):  # input images are a torch.Tensor, not a list
            orig_imgs = ops.convert_torch2numpy_batch(orig_imgs)

        # self.predictions = preds

        results = []
        path_model = 'resnet50_yolov8_load_model/resnet50_finetune.h5'
        # path_model ='resnet50_ViTs.keras'
        list_nhan = []
        count = 0
        for i, pred in enumerate(preds):

            # cv2.imwrite('/content/drive/MyDrive/HandDetection/ultralytics-8.1.9/ultralytics/models/yolo/detect/runs/detect/na.jpg', orig_imgs)
            orig_img = orig_imgs[i]
            # plt.imsave('blaaa.jpg', orig_img)
            # plt.show()
            # print('ỏggggggggg', orig_img.shape)
            # bla1 = HandDetection(path_model)
            # bla = bla1.predict(orig_img, pred)

            print('pred luc dau', pred[:, :4])
            pred[:, :4] = ops.scale_boxes(img.shape[2:], pred[:, :4], orig_img.shape)

            for box in pred[:, :4]:
                x1, y1, x2, y2 = box
                startPoint = (int(x1), int(y1))
                endPoint = (int(x2), int(y2))
                cropped_image = orig_img[int(y1):int(y2), int(x1):int(x2)]
                # print('startPointttttttt', startPoint)
                # cv2.rectangle(cropped_image, (startPoint), (endPoint), (0, 255, 0), 5)
                count = count + 1
                # cv2.imwrite(f'test2object_crop{count}.jpg', cropped_image)

                bla1 = HandDetection(path_model)
                bla = bla1.predict(cropped_image, box, orig_img)  # box pred[:, :4]
                print('pred44444444444', pred[:, :4])
                print('box44444444444', box)

                list_nhan.append(bla)
            print('predict rếntsssssssss', list_nhan)

            img_path = self.batch[0][i]
            results.append(Results(orig_img, path=img_path, names=self.model.names, boxes=pred, label_detect=bla))
            # print('preds', preds)
            # print('pred', pred)
            # for k, v in enumerate(results):
            #   print("kkkkkkkkkkk", k)
            #   print('model nameaaaaaaaaaa', v)

        return results


class HandDetection():
    def __init__(self, path_model):
        self.model = load_model(path_model)

    # def load_model(self, path):
    #     model = load_model(path)
    #     return model

    def preprocess_image(self, img):
        # img = cv2.imread(image_path)
        img_rgb = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
        img_resize = cv2.resize(img_rgb, (64, 64))  # Đảm bảo kích thước ảnh phù hợp với mô hình
        # img = img.astype('float32') / 255.0  # Chuẩn hóa giá trị pixel về khoảng [0, 1]
        img_arr = np.asarray(img_resize)
        img_final = np.expand_dims(img_arr, axis=0)  # Thêm chiều batch
        return img_final

    def predict(self, img, bbox, org_img):
        labels = ['lying', 'sitting', 'standing']
        img_preprocess = self.preprocess_image(img)
        pred = self.model.predict(img_preprocess)  # Dự đoán
        itemindex = np.where(pred == np.max(pred))
        prediction = itemindex[1][0]
        # print('shapeeeeeimg', img.shape)
        # plt.imsave('blaaa1.jpg', img)

        # drawbox each (cai nay moi doi)
        x1, y1, x2, y2 = bbox
        startPoint = (int(x1), int(y1))
        endPoint = (int(x2), int(y2))
        # print('startPointttttttt', startPoint)
        cv2.rectangle(org_img, (startPoint), (endPoint), (0, 255, 0), 5)

        cv2.putText(org_img, labels[prediction], (int(x1 + 10), int(y1 + 60)), cv2.FONT_HERSHEY_COMPLEX, 0.5,
                    (0, 255, 0), 2)
        print('nhãn', labels[prediction])

        itemindex = np.where(pred == np.max(pred))
        print('itemindex:{}'.format(itemindex))
        prediction = itemindex[1][0]
        print("probability: " + str(np.max(pred) * 100) + "%\nPredicted class : ", labels[prediction])

        # draw bbox
        print('bbox', bbox)
        # for box in bbox[:, :4]:
        #   x1, y1, x2, y2 = box
        #   startPoint = (int(x1), int(y1))
        #   endPoint = (int(x2), int(y2))
        #   # print('startPointttttttt', startPoint)
        #   cv2.rectangle(org_img, (startPoint), (endPoint), (0, 255, 0), 5)

        #   cv2.putText(org_img, labels[prediction], (int(x1 + 10), int(y1 + 60)), cv2.FONT_HERSHEY_COMPLEX, 0.5, (0, 255, 0), 2)
        #   print('nhãn', labels[prediction])
        return labels[prediction]

    # def drawbox(self, predictions):
    #   for pred in self.predictions:
    #     # Lấy tọa độ của hộp giới hạn và vẽ nó
    #     x, y, w, h = pred[:4]
    #     cv2.rectangle(image, (x, y), (x+w, y+h), (0, 255, 0), 2)
    #     # Hiển thị nhãn
    #     label = self.predict(image_path)
    #     cv2.putText(image, label, (x, y - 5), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 2)
    #   return image


# if __name__ == "__main__":
#     # from ultralytics import YOLO
#     # model = YOLO('yolov8n.pt')
#     # result = model.train(data='/content/drive/MyDrive/HandDetection/ultralytics-8.1.9/ultralytics/models/yolo/detect/phone-8/data.yaml', epochs = 2)
#     from ultralytics.utils import ASSETS
#     from ultralytics.models.yolo.detect import DetectionPredictor
#
#     # args = dict(model='/content/drive/MyDrive/HARDetection/ultralytics-8.1.9/ultralytics/models/yolo/detect/yolov8n.pt',
#     # source='/content/drive/MyDrive/HARDetection/input_data/human-activity-detection-3/test/standing/stand-717_jpg.rf.8a913aac4335049f3295e0f8e4f9930a.jpg',
#     #             classes=0
#     #  )
#     # predictor = DetectionPredictor(overrides=args)
#
#     # predictor.predict_cli()
#     # bla123 = predictor.predict_cli()
#
#     from ultralytics import YOLO
#
#     # Load a pretrained YOLOv8n model
#     model = YOLO('yolov8n.pt')
#
#     # Run inference on 'bus.jpg' with arguments
#     img_test = \
#         '/content/drive/MyDrive/HARDetection/input_data/test2object.jpg'
#
#     predict = model.predict(img_test, save=True, classes=0)
#     for r in predict:
#         print('blaaaaaaaa23143215325231', r.bla)
    # cli
#   from ultralytics import YOLO

# # Load a model
#   model = YOLO('/content/drive/MyDrive/HARDetection/ultralytics-8.1.9/ultralytics/models/yolo/detect/yolov8n.pt')  # pretrained YOLOv8n model

#






########### video
# cap = cv2.VideoCapture('/content/drive/MyDrive/HARDetection/1065673186-preview.mp4')

# # Kiểm tra xem camera đã mở thành công chưa
# if not cap.isOpened():
#   print("Không thể mở camera. Hãy chắc chắn rằng không có ứng dụng hoặc dịch vụ nào khác sử dụng camera.")

# while True:
#   # Đọc hình ảnh từ camera
#   ret, frame = cap.read()
#   args = dict(
#     model='/content/drive/MyDrive/HARDetection/ultralytics-8.1.9/ultralytics/models/yolo/detect/runs/detect/train/weights/best.pt',
#     source=frame)
#   predictor = DetectionPredictor(overrides=args)
#   predictor.predict_cli()
#   # Hiển thị hình ảnh từ camera
#   # cv2.imshow('Camera', frame)

#       # Đợi phím nhấn 'q' để thoát vòng lặp
#   if cv2.waitKey(1) & 0xFF == ord('q'):
#       break

#   # Giải phóng camera và đóng cửa sổ hiển thị
# cap.release()
# cv2.destroyAllWindows()


# path_img = '/content/drive/MyDrive/HandDetection/ultralytics-8.1.9/ultralytics/models/yolo/detect/phone-8/test/images/phone01001_png.rf.50a380246f545154fe1c76c37315d7ca.jpg'







